import logging
import sys

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from youtube_transcript_api import (
    NoTranscriptFound,
    TranscriptsDisabled,
    YouTubeTranscriptApi,
)
from youtube_transcript_api.formatters import TextFormatter

logger = logging.getLogger(__name__)


class YoutubeVideoExtractor:
    def __init__(self, api_key):
        if not api_key:
            raise ValueError("API Key is required to initialize YouTubeExtractor")
        self.youtube_client = build("youtube", "v3", developerKey=api_key)
        self.formatter = TextFormatter()
        self.transcript_client = YouTubeTranscriptApi()

    def _get_metadata(self, youtube_id):
        try:
            response = (
                self.youtube_client.videos()
                .list(part="snippet,contentDetails,topicDetails", id=youtube_id)
                .execute()
            )
            items = response.get("items", [])
            if not items:
                logger.warning(f"Video ID {youtube_id} returned no results.")
                return {}
            snippet = items[0].get("snippet", {})
            target_keys = ["title", "publishedAt", "description", "channelTitle"]
            return {key: snippet.get(key) for key in target_keys if snippet.get(key)}
        except HttpError as e:
            logger.error(f"Google API Error for {youtube_id}: {e}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error fetching metadata for {youtube_id}: {e}")
            return {}

    def _get_transcript(self, youtube_id):
        try:
            transcript_list = self.transcript_client.list(youtube_id)
            transcript = transcript_list.find_transcript(["en", "ar"]).fetch()
            return self.formatter.format_transcript(transcript)
        except (TranscriptsDisabled, NoTranscriptFound):
            logger.warning(f"Transcripts unavailable for {youtube_id}")
            return None
        except Exception as e:
            logger.error(f"Error fetching transcript for {youtube_id}: {e}")
            return None

    def extract_data(self, youtube_id):
        logger.info(f"Extracting data for: {youtube_id}...")
        data = self._get_metadata(youtube_id)
        if not data:
            logger.warning(f"failed to extract data {youtube_id}")
            return None

        data["transcript"] = self._get_transcript(youtube_id)

        logger.info(f"Successfully extracted data for {youtube_id}")
        return data


def setup_logging_config():
    LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler("myapp.log")
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    root_logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    root_logger.addHandler(console_handler)


# if __name__ == "__main__":
#     load_dotenv()

#     setup_logging_config()

#     api_key = os.getenv("GOOGLE_API")
#     if not api_key:
#         logger.critical("No GOOGLE_API key found in environment variables.")
#         sys.exit(1)

#     try:
#         extractor = YoutubeVideoExtractor(api_key)

#         test_id = "Dubq7s-5zLU"
#         result = extractor.extract_data(test_id)

#         pprint.pp(result)

#     except Exception as e:
#         logger.critical(f"Application crashed: {e}")
