import functools
import json
import logging
from datetime import datetime

from notion_client import Client
from notion_client.errors import APIResponseError

logger = logging.getLogger(__name__)


class NotionScript:
    def __init__(self, api_key, entities_db_id, media_db_id, snippet_db_id):
        if not api_key:
            raise ValueError("API Key is required to initialize NotionScript")

        try:
            self.notion_client = Client(auth=api_key)
            self.entities_db_id = entities_db_id
            self.media_db_id = media_db_id
            self.snippet_db_id = snippet_db_id
        except Exception as e:
            logger.critical("Failed to initialize Notion Client: %s", e)
            raise

    def _get_today_iso(self):
        return datetime.now().strftime("%Y-%m-%d")

    @staticmethod
    def load_data_from_json(json_path):
        logger.info("Loading %s", json_path)
        try:
            with open(json_path, "r", encoding="utf-8") as file:
                data = json.load(file)
                logger.info(
                    "Successfully loaded JSON with %d top-level keys", len(data)
                )
                return data
        except FileNotFoundError:
            logger.error("JSON file not found: %s", json_path)
            raise
        except json.JSONDecodeError as e:
            logger.error("Failed to parse JSON file: %s", e)
            raise
        except Exception as e:
            logger.error("Unexpected error loading JSON: %s", e)
            raise

    def _split_into_chunks(self, text, chunk_size=1900):
        return [text[i : i + chunk_size] for i in range(0, len(text), chunk_size)]

    def _create_code_blocks(self, chunks):
        children = []
        for chunk in chunks:
            children.append(
                {
                    "object": "block",
                    "type": "code",
                    "code": {
                        "rich_text": [
                            {
                                "type": "text",
                                "text": {"content": chunk},
                            }
                        ],
                        "language": "markdown",
                    },
                }
            )
        return children

    def prepare_summary_blocks(self, text):
        chunks = self._split_into_chunks(text, chunk_size=1900)
        logger.info("Summary split into %d chunks.", len(chunks))
        return self._create_code_blocks(chunks)

    @functools.cache
    def get_or_create_entity(self, name):
        logger.info("API CALL (CACHE MISS): Querying for entity: %s", name)
        name = name.strip()

        if not name:
            logger.warning("Empty entity name provided")
            return None

        try:
            compound_filter = {
                "or": [
                    {"property": "Aliases", "multi_select": {"contains": name}},
                    {"property": "Name", "title": {"equals": name}},
                ]
            }

            response = self.notion_client.data_sources.query(
                **{
                    "data_source_id": self.entities_db_id,
                    "filter": compound_filter,
                }
            )
            if response["results"]:
                entity_id = response["results"][0]["id"]
                logger.info("CACHE POPULATED: Found existing entity ID: %s", entity_id)
                return entity_id

        except APIResponseError as e:
            logger.error(
                "API ERROR during entity lookup: %s | Response: %s",
                e.status_code,
                e.response.json(),
            )
        except Exception as e:
            logger.error("Unexpected error during entity lookup: %s", e)

        logger.info("Entity not found. creating: %s", name)
        try:
            response = self.notion_client.pages.create(
                **{
                    "parent": {
                        "type": "data_source_id",
                        "data_source_id": self.entities_db_id,
                    },
                    "properties": {
                        "Name": {"title": [{"text": {"content": name}}]},
                        "Status": {"select": {"name": "Inbox"}},
                    },
                }
            )
            entity_id = response["id"]
            logger.info("CREATED ENTITY: %s | ID: %s", name, entity_id)
            return entity_id

        except APIResponseError as e:
            logger.error(
                "API ERROR during entity CREATION for %s: %s | Response: %s",
                name,
                e.status_code,
                e.response.json(),
            )
            return None

        except Exception as e:
            logger.error("Unexpected error during entity creation for %s: %s", name, e)
            return None

    def create_media(self, data):
        logger.info("Starting Media page creation for: %s", data["video_name"])
        entity_name = data["channel_name"].strip()
        entity_id = self.get_or_create_entity(entity_name)
        today_iso = self._get_today_iso()
        if not entity_id:
            logger.error(
                "FATAL: Could not get or create author entity. Aborting media creation."
            )
            return None

        try:
            response = self.notion_client.pages.create(
                **{
                    "parent": {
                        "type": "data_source_id",
                        "data_source_id": self.media_db_id,
                    },
                    "properties": {
                        "Title": {"title": [{"text": {"content": data["video_name"]}}]},
                        "Media Type": {"select": {"name": "Video"}},
                        "Author/Creator": {"relation": [{"id": entity_id}]},
                        "URL": {"url": data["url"]},
                        "Publishing Date": {"date": {"start": data["upload_date"]}},
                        "Adding Date": {"date": {"start": today_iso}},
                        "Status": {"select": {"name": "Inbox"}},
                    },
                    "children": self.prepare_summary_blocks(data["full_summary"]),
                }
            )
            media_page_id = response["id"]
            logger.info("CREATED MEDIA: %s | ID: %s", data["video_name"], media_page_id)

            for snippet in data["extracted_snippets"]:
                self.create_snippet(snippet, media_page_id)

            return media_page_id

        except APIResponseError as e:
            logger.error(
                "API ERROR creating media page: %s | Response: %s",
                e.status_code,
                e.response.json(),
            )
            return None
        except Exception as e:
            logger.error("Unexpected error during media creation: %s", e)
            return None

    def create_snippet(self, snippet, media_source):
        entities = []
        today_iso = self._get_today_iso()
        for entity_name in snippet.get("entities", []):
            linked_entity = self.get_or_create_entity(entity_name)
            if linked_entity:
                entities.append({"id": linked_entity})
        properties = {
            "Context": {"title": [{"text": {"content": snippet["context"]}}]},
            "Source": {"relation": [{"id": media_source}]},
            "Entities": {"relation": entities},
            "Note Type": {"select": {"name": "Automated Note"}},
            "Status": {"select": {"name": "Inbox"}},
            "Adding Date": {"date": {"start": today_iso}},
        }
        if snippet["event_date"].get("human_readable"):
            properties["Event Date"] = {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {"content": snippet["event_date"]["human_readable"]},
                    }
                ]
            }
        if snippet["event_date"].get("date_start_iso"):
            properties["Start Date"] = {
                "date": {"start": snippet["event_date"]["date_start_iso"]}
            }
        if snippet["event_date"].get("date_end_iso"):
            properties["End Date"] = {
                "date": {"start": snippet["event_date"]["date_end_iso"]}
            }
        try:
            response = self.notion_client.pages.create(
                **{
                    "parent": {
                        "type": "data_source_id",
                        "data_source_id": self.snippet_db_id,
                    },
                    "properties": properties,
                }
            )
            logger.info(
                "CREATED SNIPPET: %s | Source: %s",
                snippet["context"][:50],
                media_source,
            )
            return response
        except APIResponseError as e:
            logger.error(
                "API ERROR creating snippet: %s | Context: %s",
                e.status_code,
                snippet["context"][:50],
            )
            return None
