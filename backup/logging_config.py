
import logging
import sys
import os
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler

def configure_logging():
    """
    Configure logging for the backup service.
    Includes timestamped file logging, detailed backup operation logs, and appropriate log levels.
    """
    # Create logs directory if it doesn't exist
    log_dir = "/app/logs"
    os.makedirs(log_dir, exist_ok=True)

    # Create formatters
    console_formatter = logging.Formatter(
        "%(asctime)s  %(name)-22s  %(levelname)-8s  %(message)s"
    )

    # Detailed formatter for file logs with more context
    file_formatter = logging.Formatter(
        "%(asctime)s  %(name)s  %(levelname)s  %(filename)s:%(lineno)d  %(funcName)s  %(message)s"
    )

    # Console handler for stdout
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)

    # Timed rotating file handler - rotates daily and keeps 30 days
    current_date = datetime.now().strftime("%Y-%m-%d")
    log_filename = f"{log_dir}/backup_{current_date}.log"
    file_handler = TimedRotatingFileHandler(
        log_filename,
        when="midnight",  # Rotate at midnight
        interval=1,
        backupCount=30,  # Keep 30 days of logs
        encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)

    # Separate handler for errors/warnings only
    error_handler = TimedRotatingFileHandler(
        f"{log_dir}/backup_errors_{current_date}.log",
        when="midnight",
        interval=1,
        backupCount=30,
        encoding="utf-8"
    )
    error_handler.setLevel(logging.WARNING)
    error_handler.setFormatter(file_formatter)

    # Configure root logger
    logging.basicConfig(
        level=logging.DEBUG,
        handlers=[console_handler, file_handler, error_handler]
    )

    # Set specific log levels for different components
    logging.getLogger("apscheduler").setLevel(logging.INFO)
    logging.getLogger("apscheduler.executors").setLevel(logging.WARNING)  # Reduce scheduler noise
    logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("google.cloud.storage").setLevel(logging.WARNING)

    # Backup-specific loggers
    logging.getLogger("gcs_service").setLevel(logging.INFO)
    logging.getLogger("__main__").setLevel(logging.DEBUG)  # Main backup script gets debug level
