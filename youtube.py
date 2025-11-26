import logging

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from youtube_transcript_api import (
    NoTranscriptFound,
    TranscriptsDisabled,
    YouTubeTranscriptApi,
)
from youtube_transcript_api.formatters import TextFormatter

logger = logging.getLogger(__name__)


class YoutubeExtractor:
    def __init__(self, api_key):
        if not api_key:
            logger.error("API Key is missing for YouTubeExtractor initialization.")
            raise ValueError("API Key is required to initialize YouTubeExtractor")
        self.youtube_client = build("youtube", "v3", developerKey=api_key)
        self.formatter = TextFormatter()
        self.transcript_client = YouTubeTranscriptApi()
        logger.info("YoutubeExtractor initialized.")

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
            logger.info(f"Successfully fetched metadata for {youtube_id}.")
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
            logger.info(f"Successfully fetched transcript for {youtube_id}.")
            return self.formatter.format_transcript(transcript).splitlines()

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

        data["url"] = f"https://www.youtube.com/watch?v={youtube_id}"
        data["transcript"] = self._get_transcript(youtube_id)
        logger.info(f"Successfully extracted data for {youtube_id}")
        return data
