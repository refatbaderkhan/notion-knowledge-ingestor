import json
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

import config
from gemini import GeminiProcessor
from notion import NotionIngester
from youtube import YoutubeExtractor

load_dotenv()


def setup_logging():
    LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler("myapp.log")
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(console_handler)

    return logger


def load_youtube_ids(file_path, logger):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            ids = json.load(f)
            if not isinstance(ids, list) or not all(isinstance(i, str) for i in ids):
                logger.critical(
                    f"Input file {file_path} must contain a list of strings (YouTube IDs)."
                )
                sys.exit(1)
            return ids
    except FileNotFoundError:
        logger.critical(
            f"Input file not found: {file_path}. Please create it with a list of IDs."
        )
        sys.exit(1)
    except json.JSONDecodeError:
        logger.critical(
            f"Error decoding JSON from {file_path}. Check for syntax errors."
        )
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Unexpected error loading IDs: {e}", exc_info=True)
        sys.exit(1)


def process_video(
    youtube_id, youtube_extractor, gemini_processor, notion_ingester, logger
):
    try:
        logger.info("-" * 40)
        logger.info("Processing YouTube ID: %s", youtube_id)
        logger.info("-" * 40)

        youtube_data = youtube_extractor.extract_data(youtube_id)
        if not youtube_data:
            logger.error("Skipping video due to failed data extraction.")
            return

        gemini_data = gemini_processor.summarize_video("prompt.txt", youtube_data)
        if not gemini_data:
            logger.error("Skipping video due to failed Gemini summarization.")
            return

        full_data = dict(youtube_data)
        full_data.update(gemini_data)

        output_folder = Path("output_data")
        output_folder.mkdir(parents=True, exist_ok=True)
        file_path = output_folder / f"{youtube_id}-full.json"

        with open(file_path, "w", encoding="utf-8") as json_file:
            json.dump(full_data, json_file, indent=4, ensure_ascii=False)

        logger.info(f"Successfully dumped combined data to JSON file at: {file_path}")

        result = notion_ingester.create_media(full_data)

        if result:
            logger.info("SUCCESS: Workflow completed. Media Page ID: %s", result)
        else:
            logger.error("FAILURE: Workflow finished, but Media Page was not created.")

    except Exception as e:
        logger.critical("Workflow failed for %s: %s", youtube_id, e, exc_info=True)


def main():
    logger = setup_logging()
    logger.info("=" * 60)
    logger.info("Starting Notion media import batch workflow")
    logger.info("=" * 60)

    notion_api_key = os.getenv("NOTION_API_KEY")
    youtube_api_key = os.getenv("GOOGLE_API")

    if not notion_api_key or not youtube_api_key:
        logger.critical(
            "One or more required API keys are missing from environment (.env file)."
        )
        sys.exit(1)

    try:
        youtube_extractor = YoutubeExtractor(api_key=youtube_api_key)
        gemini_processor = GeminiProcessor()
        notion_ingester = NotionIngester(
            api_key=notion_api_key,
            entities_db_id=config.ENTITIES_DB_ID,
            media_db_id=config.MEDIA_DB_ID,
            snippet_db_id=config.SNIPPETS_DB_ID,
        )

        youtube_ids = load_youtube_ids(config.YOUTUBE_IDS_FILE, logger)
        logger.info(
            "Loaded %d YouTube IDs from %s.", len(youtube_ids), config.YOUTUBE_IDS_FILE
        )

        for video_id in youtube_ids:
            process_video(
                video_id, youtube_extractor, gemini_processor, notion_ingester, logger
            )

        logger.info("=" * 60)
        logger.info("Batch workflow finished.")
        logger.info("=" * 60)

    except Exception as e:
        logger.critical(
            "CRITICAL FAILURE during processor initialization or batch start: %s",
            e,
            exc_info=True,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
