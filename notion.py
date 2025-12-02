import functools
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from notion_client import Client
from notion_client.errors import APIResponseError

import config

logger = logging.getLogger(__name__)


class NotionIngester:
    def __init__(self):
        if not config.NOTION_API_KEY:
            raise ValueError("NOTION_API_KEY is required.")

        self.client = Client(auth=config.NOTION_API_KEY)

        self.entities_db = config.ENTITIES_DB_ID
        self.media_db = config.MEDIA_DB_ID
        self.snippet_db = config.SNIPPETS_DB_ID

    def _get_today_iso(self) -> str:
        return datetime.now().strftime("%Y-%m-%d")

    def _prepare_markdown_blocks(self, text: str) -> List[Dict[str, Any]]:
        chunk_size = 1900
        chunks = [text[i : i + chunk_size] for i in range(0, len(text), chunk_size)]

        return [
            {
                "object": "block",
                "type": "code",
                "code": {
                    "rich_text": [{"type": "text", "text": {"content": chunk}}],
                    "language": "markdown",
                },
            }
            for chunk in chunks
        ]

    @functools.cache
    def get_or_create_entity(self, name: str) -> Optional[str]:
        name = name.strip()
        if not name:
            return None

        logger.info(f"Resolving entity: {name}")

        try:
            response = self.client.data_sources.query(
                **{
                    "data_source_id": self.entities_db,
                    "filter": {
                        "or": [
                            {"property": "Aliases", "multi_select": {"contains": name}},
                            {"property": "Name", "title": {"equals": name}},
                        ]
                    },
                }
            )

            if response["results"]:
                entity_id = response["results"][0]["id"]
                logger.info("Found existing entity ID: %s", entity_id)
                return entity_id

            logger.info("Entity not found. creating: %s", name)

            new_page = self.client.pages.create(
                **{
                    "parent": {
                        "type": "data_source_id",
                        "data_source_id": self.entities_db,
                    },
                    "properties": {
                        "Name": {"title": [{"text": {"content": name}}]},
                        "Status": {"select": {"name": "Inbox"}},
                    },
                }
            )

            entity_id = new_page["id"]
            logger.info("CREATED ENTITY: %s | ID: %s", name, entity_id)
            return entity_id

        except Exception as e:
            logger.error(f"Error resolving entity '{name}': {e}")
            return None

    def create_media(self, data: Dict[str, Any]) -> Optional[str]:
        logger.info(f"Creating Media page: {data.get('title')}")
        entity_name = data["channelTitle"].strip()

        entity_id = self.get_or_create_entity(entity_name)

        if not entity_id:
            logger.error(
                "FATAL: Could not get or create author entity. Aborting media creation."
            )
            return None

        try:
            response = self.client.pages.create(
                **{
                    "parent": {
                        "type": "data_source_id",
                        "data_source_id": self.media_db,
                    },
                    "properties": {
                        "Title": {"title": [{"text": {"content": data["title"]}}]},
                        "Media Type": {"select": {"name": "Video"}},
                        "Author/Creator": {"relation": [{"id": entity_id}]},
                        "URL": {"url": data["url"]},
                        "Publishing Date": {"date": {"start": data["publishedAt"]}},
                        "Adding Date": {"date": {"start": self._get_today_iso()}},
                        "Status": {"select": {"name": "Inbox"}},
                    },
                    # Note: verify _prepare_markdown_blocks is named correctly in your class
                    # (In your provided file it was named _prepare_markdown_blocks, but called as prepare_summary_blocks?
                    #  I will assume you use the name defined in this file: _prepare_markdown_blocks)
                    "children": self._prepare_markdown_blocks(data["full_summary"]),
                }
            )
            media_page_id = response["id"]
            logger.info("CREATED MEDIA: %s | ID: %s", data["title"], media_page_id)

            snippets = data.get("extracted_snippets", [])
            logger.info(f"Processing {len(snippets)} snippets...")

            for snippet in reversed(snippets):
                self._create_snippet(snippet, media_page_id)

            return media_page_id

        except Exception as e:
            logger.error(f"Failed to create media page: {e}", exc_info=True)
            return None

    def _create_snippet(self, snippet: Dict[str, Any], media_id: str) -> None:
        entities = []
        for entity_name in snippet.get("entities", []):
            linked_entity = self.get_or_create_entity(entity_name)
            if linked_entity:
                entities.append({"id": linked_entity})

        full_context = snippet["context"]
        title_content = full_context
        children_blocks = []

        if len(full_context) > 1900:
            title_content = full_context[:1900] + "... (truncated, see body)"

            chunk_size = 1900
            text_chunks = [
                full_context[i : i + chunk_size]
                for i in range(0, len(full_context), chunk_size)
            ]

            for chunk in text_chunks:
                children_blocks.append(
                    {
                        "object": "block",
                        "type": "paragraph",
                        "paragraph": {
                            "rich_text": [{"type": "text", "text": {"content": chunk}}]
                        },
                    }
                )

        properties = {
            "Context": {"title": [{"text": {"content": title_content}}]},
            "Source": {"relation": [{"id": media_id}]},
            "Entities": {"relation": entities},
            "Note Type": {"select": {"name": "Automated Note"}},
            "Status": {"select": {"name": "Inbox"}},
            "Adding Date": {"date": {"start": self._get_today_iso()}},
        }

        event_data = snippet.get("event_date", {})
        if event_data.get("human_readable") and event_data["human_readable"] != "null":
            properties["Event Date"] = {
                "rich_text": [{"text": {"content": event_data["human_readable"]}}]
            }

        date_mapping = [("date_start_iso", "Start Date"), ("date_end_iso", "End Date")]

        for json_key, notion_column in date_mapping:
            val = event_data.get(json_key)

            if val and isinstance(val, str) and val.lower() != "null":
                try:
                    datetime.strptime(val, "%Y-%m-%d")
                    properties[notion_column] = {"date": {"start": val}}
                except ValueError:
                    logger.warning(
                        f"Skipping {notion_column}: Invalid format '{val}' in snippet."
                    )

        create_kwargs = {
            "parent": {"data_source_id": self.snippet_db, "type": "data_source_id"},
            "properties": properties,
        }

        if children_blocks:
            create_kwargs["children"] = children_blocks

        try:
            return self.client.pages.create(**create_kwargs)

        except APIResponseError as e:
            if e.status == 400:
                logger.warning(
                    f"400 Error detected ({e.message}). Retrying without dates."
                )
                properties.pop("Start Date", None)
                properties.pop("End Date", None)
                create_kwargs["properties"] = properties

                try:
                    return self.client.pages.create(**create_kwargs)
                except Exception as retry_e:
                    logger.error(f"Retry failed: {retry_e}")
                    return None

            logger.error(f"Failed to create snippet: {e}")
            return None
