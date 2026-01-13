import os
import logging
import warnings
from logging.handlers import TimedRotatingFileHandler
from db_utils.early_email import NoAuthSMTPHandler, file_formatter

def setup_bootstrap_logger(
        log_file_name: str = "early_alert.log",
        log_dir: str = "C:/ELT/logs/early_logs",
        use_email_alerts: bool = True,
) -> logging.Logger:
    logger = logging.getLogger("bootstrap_logger")
    logger.setLevel(logging.DEBUG)

    # Avoid duplicate handlers
    if logger.handlers:
        return logger

    # Ensure log directory exists
    try:
        os.makedirs(log_dir, exist_ok=True)
    except Exception as e:
        warnings.warn(f"Failed to create early log directory: {e}")

    # Add rotating file handler (rotates monthly)
    log_path = os.path.join(log_dir, log_file_name)
    try:
        file_handler = TimedRotatingFileHandler(
            filename=log_path,
            when="midnight",
            interval=30,  # Roughly monthly
            backupCount=12,
            encoding="utf-8"
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    except Exception as e:
        warnings.warn(f"Failed to set up rotating file handler: {e}")

    # Add early email handler if enabled
    if use_email_alerts:
        try:
            email_handler = NoAuthSMTPHandler()
            email_handler.setLevel(logging.CRITICAL)
            email_handler.setFormatter(file_formatter)
            logger.addHandler(email_handler)
        except Exception as e:
            warnings.warn(f"Failed to set up email alert handler: {e}")

    return logger
