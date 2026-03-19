"""
Google Cloud Storage Service for database backup uploads.
Handles authentication and file operations with GCS.
"""

import logging
import os
import json
import base64
import tempfile
from datetime import datetime, timedelta, timezone
from typing import Optional
from google.cloud import storage

# Initialize logger
logger = logging.getLogger(__name__)


class GoogleCloudStorageService:
    """Service class for Google Cloud Storage backup operations."""

    def __init__(self):
        self.client: Optional[storage.Client] = None
        self.bucket_name = os.getenv("GCS_DB_BACKUP_BUCKET_NAME", "vps_postgres_backups")
        self._temp_credentials_file: Optional[str] = None
        self._initialize_service()

    def _initialize_service(self):
        """Initialize the Google Cloud Storage client using base64-encoded credentials."""
        credentials_b64 = os.getenv("GCP_SERVICE_ACCOUNT_CREDENTIALS")

        if not credentials_b64:
            logger.warning("GCP_SERVICE_ACCOUNT_CREDENTIALS environment variable not set")
            return

        try:
            # Decode the base64 credentials
            credentials_json = base64.b64decode(credentials_b64).decode("utf-8")
            credentials_dict = json.loads(credentials_json)

            # Create a temporary file for google-cloud-storage to use
            # The library expects GOOGLE_APPLICATION_CREDENTIALS env var pointing to a file
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".json", delete=False
            ) as f:
                json.dump(credentials_dict, f)
                self._temp_credentials_file = f.name

            # Set the environment variable for google-cloud-storage
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self._temp_credentials_file

            # Create the GCS client
            self.client = storage.Client()
            logger.info(
                f"Google Cloud Storage service initialized successfully (bucket: {self.bucket_name})"
            )

        except Exception as e:
            logger.error(f"Error initializing Google Cloud Storage service: {e}")
            self.client = None

    def is_available(self) -> bool:
        """Check if the Google Cloud Storage service is available."""
        return self.client is not None and self.bucket_name is not None

    def upload_file(self, filepath: str) -> bool:
        """
        Upload a file to the configured GCS bucket.

        Args:
            filepath: Path to the file to upload

        Returns:
            True if successful, False otherwise
        """
        if not self.is_available():
            logger.error("Google Cloud Storage service not available")
            return False

        filename = os.path.basename(filepath)
        logger.info(f"Uploading {filename} to GCS bucket: {self.bucket_name}...")

        try:
            bucket = self.client.bucket(self.bucket_name)
            blob = bucket.blob(filename)
            blob.upload_from_filename(filepath)

            logger.info(f"Successfully uploaded: {filename}")
            return True

        except Exception as e:
            logger.error(f"Error during GCS upload: {e}")
            return False

    def cleanup_old_backups(self, retention_days: int) -> int:
        """
        Delete files in the GCS bucket older than retention_days.

        Args:
            retention_days: Number of days to keep backups

        Returns:
            Number of files deleted
        """
        if not self.is_available():
            logger.error("Google Cloud Storage service not available")
            return 0

        logger.info(
            f"Checking for backups older than {retention_days} days in GCS bucket: {self.bucket_name}..."
        )

        # Calculate the cutoff date (must be timezone-aware for comparison with GCS metadata)
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=retention_days)

        deleted_count = 0

        try:
            bucket = self.client.bucket(self.bucket_name)
            # List all blobs in the bucket
            blobs = bucket.list_blobs()

            for blob in blobs:
                # blob.time_created is a timezone-aware datetime object
                if blob.time_created < cutoff_date:
                    blob_name = blob.name
                    logger.info(
                        f"-> Deleting old backup: {blob_name} (Created: {blob.time_created.date()})"
                    )
                    try:
                        blob.delete()
                        deleted_count += 1
                    except Exception as e:
                        logger.error(f"Error deleting blob {blob_name}: {e}")

        except Exception as e:
            logger.error(f"Error during GCS cleanup: {e}")

        logger.info(f"Cleanup complete. Deleted {deleted_count} old backup file(s).")
        return deleted_count

    def __del__(self):
        """Cleanup temporary credentials file on destruction."""
        if self._temp_credentials_file and os.path.exists(self._temp_credentials_file):
            try:
                os.unlink(self._temp_credentials_file)
                # Also clean up the env var if it points to our temp file
                if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") == self._temp_credentials_file:
                    del os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
            except Exception as e:
                logger.warning(f"Failed to delete temp credentials file: {e}")
