import functools
import json
import logging
import os
import sys
from datetime import datetime

from dotenv import load_dotenv
from notion_client import Client
from notion_client.errors import APIResponseError

import config

# Load enviroment variables
load_dotenv()


# Logging
LOG_FORMAT = (
    "%(asctime)s | %(levelname)s | [%(name)s:%(funcName)s:%(lineno)d] | %(message)s"
)


def setup_logging():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler("myapp.log")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT))

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


logger = setup_logging()


# Notion connection
def initialize_notion_client() -> Client:
    try:
        api_key = os.getenv("NOTION_API_KEY")
        if not api_key:
            raise ValueError("NOTION_API_KEY not found in environment variables")
        return Client(auth=api_key)
    except Exception as e:
        logger.critical("Failed to initialize Notion Client: %s", e)
        sys.exit(1)


NOTION_CLIENT = initialize_notion_client()


# Helpers
def get_today_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def load_data_from_json(json_path):
    logger.info("Loading %s", json_path)
    try:
        with open(json_path, "r", encoding="utf-8") as file:
            data = json.load(file)
            logger.info("Successfully loaded JSON with %d top-level keys", len(data))
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


def split_into_chunks(text, chunk_size=1900):
    return [text[i : i + chunk_size] for i in range(0, len(text), chunk_size)]


def create_code_blocks(chunks):
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


def prepare_summary_blocks(text):
    chunks = split_into_chunks(text, chunk_size=1900)
    logger.info("Summary split into %d chunks.", len(chunks))
    children = create_code_blocks(chunks)
    return children


# Notion logic


@functools.cache
def get_or_create_entity(name):
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

        response = NOTION_CLIENT.data_sources.query(
            **{
                "data_source_id": config.ENTITIES_DB_ID,
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
        response = NOTION_CLIENT.pages.create(
            **{
                "parent": {
                    "type": "data_source_id",
                    "data_source_id": config.ENTITIES_DB_ID,
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


def create_media(data):
    logger.info("Starting Media page creation for: %s", data["video_name"])
    entity_name = data["channel_name"].strip()
    entity_id = get_or_create_entity(entity_name)
    today_iso = get_today_iso()
    if not entity_id:
        logger.error(
            "FATAL: Could not get or create author entity. Aborting media creation."
        )
        return None

    try:
        response = NOTION_CLIENT.pages.create(
            **{
                "parent": {
                    "type": "data_source_id",
                    "data_source_id": config.MEDIA_DB_ID,
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
                "children": prepare_summary_blocks(data["full_summary"]),
            }
        )
        media_page_id = response["id"]
        logger.info("CREATED MEDIA: %s | ID: %s", data["video_name"], media_page_id)

        for snippet in data["extracted_snippets"]:
            create_snippet(snippet, media_page_id)

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


def create_snippet(snippet, media_source):
    entities = []
    today_iso = get_today_iso()
    for entity_name in snippet.get("entities", []):
        linked_entity = get_or_create_entity(entity_name)
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
        response = NOTION_CLIENT.pages.create(
            **{
                "parent": {
                    "type": "data_source_id",
                    "data_source_id": config.SNIPPETS_DB_ID,
                },
                "properties": properties,
            }
        )
        logger.info(
            "CREATED SNIPPET: %s | Source: %s", snippet["context"][:50], media_source
        )
        return response
    except APIResponseError as e:
        logger.error(
            "API ERROR creating snippet: %s | Context: %s",
            e.status_code,
            snippet["context"][:50],
        )
        return None


def main():
    logger.info("=" * 60)
    logger.info("Starting Notion media import workflow")
    logger.info("=" * 60)

    try:
        input_data = load_data_from_json(config.JSON_FILE_NAME)
        result = create_media(input_data)

        if result:
            logger.info("=" * 60)
            logger.info("Workflow completed successfully!")
            logger.info("Media page ID: %s", result)
            logger.info("=" * 60)
        else:
            logger.error("Workflow completed with errors")
            sys.exit(1)

    except Exception as e:
        logger.critical("Workflow failed: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
