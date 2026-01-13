# shared_libs/db_utils/early_email.py
import os
import logging
import smtplib
import ssl
from logging import Handler
from logging import Formatter
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

# Set up path
# Load .env safely before anything else
try:
    if os.path.exists("/opt/early_alert/.env"):  # Docker path
        load_dotenv(dotenv_path="/opt/early_alert/.env")
    else:
        local_path = "C:/ELT/.early_alert/.env"
        load_dotenv(dotenv_path=local_path)
except Exception as e:
    print(f"[early_email] Failed to load .env: {e}")

# Shared formatter
base_format = "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"
date_format = "%Y-%m-%d %H:%M:%S"
file_formatter = Formatter(base_format, datefmt=date_format)

class NoAuthSMTPHandler(Handler):
    def emit(self, record):
        try:
            ssl_context = ssl.create_default_context()
            email_host = os.getenv("EARLY_ALERT_MAILHOST", "localhost")
            email_port = int(os.getenv("EARLY_ALERT_MAILPORT", 465))
            email_from = os.getenv("EARLY_ALERT_FROM", "log@yourapp.com")
            email_to_string = os.getenv("EARLY_ALERT_USER", "admin@yourapp.com")

            message = self.format(record)
            msg = MIMEMultipart()
            msg["From"] = email_from
            msg["To"] = email_to_string
            msg["Subject"] = "Early Alert Notification"
            msg.attach(MIMEText(message, "plain"))

            email_to_list = email_to_string.split(",")
            with smtplib.SMTP_SSL(email_host, email_port, context=ssl_context) as server:
                server.sendmail(email_from, email_to_list, msg.as_string())

        except Exception:
            self.handleError(record)

def get_early_alert_logger(name="early_alert") -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.CRITICAL)

    if not any(isinstance(h, NoAuthSMTPHandler) for h in logger.handlers):
        handler = NoAuthSMTPHandler()
        handler.setLevel(logging.CRITICAL)
        handler.setFormatter(file_formatter)
        logger.addHandler(handler)

    return logger

def send_early_email_alert(subject: str, message: str):
    logger = get_early_alert_logger()
    logger.critical(message)
    print("Pre-process email alert triggered (if configured).")
