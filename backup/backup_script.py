"""
Database backup script with scheduled execution.
Creates PostgreSQL backups and uploads them to Google Cloud Storage.
"""

import os
import subprocess
import datetime
import logging
import sys
from urllib.parse import urlparse
from zoneinfo import ZoneInfo
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from gcs_service import GoogleCloudStorageService
from logging_config import configure_logging

# Configure logging with same config as backend
configure_logging()
logger = logging.getLogger(__name__)

# PostgreSQL connection details (from environment variables)
# Support both individual POSTGRES_* vars and DATABASE_URL
if os.environ.get("DATABASE_URL"):
    # Parse DATABASE_URL: postgresql://user:password@host:port/dbname
    database_url = os.environ.get("DATABASE_URL")
    parsed = urlparse(database_url)
    DB_HOST = parsed.hostname or "postgres"
    DB_PORT = str(parsed.port) if parsed.port else "5432"
    DB_USER = parsed.username
    DB_PASS = parsed.password
    DB_NAME = (parsed.path.lstrip("/") if parsed.path else None) or "postgres"
else:
    # Use individual environment variables
    DB_HOST = os.environ.get("POSTGRES_HOST", "postgres")
    DB_PORT = os.environ.get("POSTGRES_PORT", "5432")
    DB_USER = os.environ.get("POSTGRES_USER")
    DB_PASS = os.environ.get("POSTGRES_PASSWORD")
    DB_NAME = os.environ.get("POSTGRES_DB", "postgres")

# Backup configuration
RETENTION_DAYS = int(os.environ.get("BACKUP_RETENTION_DAYS", "30"))
BACKUP_SCHEDULE_HOUR = os.environ.get("BACKUP_SCHEDULE_HOUR", "0,12")
BACKUP_SCHEDULE_MINUTE = os.environ.get("BACKUP_SCHEDULE_MINUTE", "0")
BACKUP_TIMEZONE = os.environ.get("BACKUP_TIMEZONE", "UTC")


def create_pg_dump():
    """Executes pg_dumpall to create a compressed backup of all databases."""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    backup_filename = f"backup_{timestamp}.sql.gz"

    # Set environment variable for pg_dumpall password
    os.environ["PGPASSWORD"] = DB_PASS

    # pg_dumpall dumps entire cluster (all databases, roles, tablespaces)
    pg_dumpall_command = [
        "pg_dumpall",
        "-h",
        DB_HOST,
        "-p",
        DB_PORT,
        "-U",
        DB_USER,
        "-d",
        DB_NAME,
    ]

    # Execute pg_dumpall and pipe the output to gzip
    logger.info(f"Creating full cluster dump (all databases): {backup_filename}...")
    try:
        pg_dumpall_process = subprocess.Popen(
            pg_dumpall_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        with open(backup_filename, "wb") as f_out:
            gzip_process = subprocess.run(
                ["gzip"], stdin=pg_dumpall_process.stdout, stdout=f_out, check=True
            )

        pg_dumpall_process.stdout.close()
        pg_dumpall_process.wait()

        if pg_dumpall_process.returncode != 0:
            stderr = pg_dumpall_process.stderr.read().decode("utf-8")
            raise subprocess.CalledProcessError(
                pg_dumpall_process.returncode, pg_dumpall_command, stderr=stderr
            )

        # Get file size for logging
        file_size = os.path.getsize(backup_filename)
        logger.info(
            f"Successfully created dump file: {backup_filename} ({file_size / 1024 / 1024:.2f} MB)"
        )
        return backup_filename

    except subprocess.CalledProcessError as e:
        logger.error(f"Error during pg_dumpall/gzip: {e}")
        if hasattr(e, "stderr") and e.stderr:
            logger.error(f"pg_dumpall stderr: {e.stderr}")
        return None
    finally:
        # Clean up the password variable
        if "PGPASSWORD" in os.environ:
            del os.environ["PGPASSWORD"]


def run_backup():
    """Main backup execution function."""
    logger.info("=" * 60)
    logger.info("Starting scheduled database backup")
    logger.info("=" * 60)

    # Validate required environment variables
    if not all([DB_HOST, DB_USER, DB_PASS]):
        logger.error("ERROR: Missing required database environment variables.")
        logger.error(f"DB_HOST: {DB_HOST}, DB_USER: {DB_USER}, DB_PASS: {'*' if DB_PASS else None}")
        return

    # 1. Create PostgreSQL dump
    backup_file = create_pg_dump()

    if not backup_file:
        logger.error("Failed to create cluster dump. Aborting backup.")
        return

    try:
        # 2. Initialize Google Cloud Storage service
        gcs_service = GoogleCloudStorageService()

        if not gcs_service.is_available():
            logger.error("Google Cloud Storage service not available. Aborting backup.")
            return

        # 3. Upload to Google Cloud Storage
        upload_success = gcs_service.upload_file(backup_file)

        if upload_success:
            logger.info("Backup uploaded successfully to Google Cloud Storage")

            # 4. Cleanup old backups
            deleted_count = gcs_service.cleanup_old_backups(RETENTION_DAYS)
            logger.info(f"Cleanup completed. Deleted {deleted_count} old backup(s).")
        else:
            logger.error("Failed to upload backup to Google Cloud Storage")

    except Exception as e:
        logger.error(f"A critical error occurred during backup: {e}", exc_info=True)

    finally:
        # 5. Local cleanup (always remove the local backup file)
        if os.path.exists(backup_file):
            try:
                os.remove(backup_file)
                logger.info(f"Removed local backup file: {backup_file}")
            except Exception as e:
                logger.warning(f"Failed to remove local backup file: {e}")

    logger.info("=" * 60)
    logger.info("Backup process completed")
    logger.info("=" * 60)


def main():
    """Main function to start the backup scheduler."""
    logger.info("Initializing backup scheduler...")
    logger.info(f"Cluster: all databases @ {DB_HOST}:{DB_PORT}")
    logger.info(f"Retention: {RETENTION_DAYS} days")

    # Get timezone
    try:
        tz = ZoneInfo(BACKUP_TIMEZONE)
        logger.info(f"Schedule: hour={BACKUP_SCHEDULE_HOUR} minute={BACKUP_SCHEDULE_MINUTE} {BACKUP_TIMEZONE}")
    except Exception as e:
        logger.error(f"Invalid timezone: {BACKUP_TIMEZONE}. Error: {e}")
        logger.info("Falling back to UTC")
        tz = ZoneInfo("UTC")

    # Validate environment variables
    if not all([DB_HOST, DB_USER, DB_PASS]):
        logger.error("Missing required database environment variables. Exiting.")
        sys.exit(1)

    # Create scheduler with timezone
    scheduler = BlockingScheduler(timezone=tz)

    # Schedule the backup job with timezone
    scheduler.add_job(
        run_backup,
        trigger=CronTrigger(hour=BACKUP_SCHEDULE_HOUR, minute=BACKUP_SCHEDULE_MINUTE, timezone=tz),
        id="twice_daily_backup",
        name="Twice Daily Database Backup",
        replace_existing=True,
    )

    # Get the job to check next run time
    job = scheduler.get_job("twice_daily_backup")
    if job and hasattr(job, "next_run_time") and job.next_run_time:
        logger.info(f"Next backup scheduled for: {job.next_run_time}")
    else:
        logger.info("Backup scheduler configured. Next run time will be calculated when scheduler starts.")

    try:
        # Run an immediate backup on startup (optional - can be removed if not desired)
        logger.info("Running initial backup on startup...")
        run_backup()

        logger.info("Backup scheduler started. Waiting for scheduled time...")
        # Start the scheduler (this will block)
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped by user")
        scheduler.shutdown()
    except Exception as e:
        logger.error(f"Scheduler error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
