"""
logger.py

Provides a robust logging setup for Python applications. Supports console logging,
file logging with rotation, JSON-formatted logs, colorized development logs, and optional
email alerts on critical errors. Integrates context-aware logging via `LoggerAdapter`.

Usage:
    from logger import get_logger, configure_logging
    configure_logging(run_env='DEV', email_alerts=True, context={'job': 'ETL'})
    logger = get_logger(__name__)
    logger.info("This is a test log.")
"""

# /shared_libs/db_utils/logger.py

import os
import sys
import json
import logging
import ssl
import smtplib
import functools
from logging.handlers import TimedRotatingFileHandler, SMTPHandler
from pathlib import Path
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
from db_utils.env_utils import resolve_dotenv_path


# === Optional Dependencies ===
try:
    from rich.logging import RichHandler
    has_rich = True
except ImportError:
    RichHandler = None
    has_rich = False

try:
    from colorlog import ColoredFormatter
except ImportError:
    ColoredFormatter = None

try:
    from pythonjsonlogger import jsonlogger
except ImportError:
    jsonlogger = None

# === Setup custom level and logger method for special warning emails
STATUS_EMAIL_LEVEL = 35
logging.addLevelName(STATUS_EMAIL_LEVEL, "STATUS_EMAIL")

def status_email(self, message, *args, **kwargs):
    if self.isEnabledFor(STATUS_EMAIL_LEVEL):
        self._log(STATUS_EMAIL_LEVEL, message, args, **kwargs)

logging.Logger.status_email = status_email  # Add method to Logger class

# === Fallback Color Formatter ===
class ColorFormatter(logging.Formatter):
    COLORS = {
        logging.DEBUG: "\033[36m",     # Cyan
        logging.INFO: "\033[32m",      # Green
        logging.WARNING: "\033[33m",   # Yellow
        logging.ERROR: "\033[31m",     # Red
        logging.CRITICAL: "\033[1;41m" # Bold red background
    }
    RESET = "\033[0m"

    def format(self, record):
        color = self.COLORS.get(record.levelno, "")
        message = super().format(record)
        return f"{color}{message}{self.RESET}"

# === Context-Aware LoggerAdapter ===
class ContextLoggerAdapter(logging.LoggerAdapter):
    """
    Injects contextual information into log records.
    Used to enrich log messages with metadata like job name, user, or environment.
    """
    def process(self, msg, kwargs):
        context_str = " ".join(f"{k}={v}" for k, v in self.extra.items())
        return f"[{context_str}] {msg}", kwargs

    def status_email(self, msg, *args, **kwargs):
        """
        Support custom STATUS_EMAIL log level.
        """
        self.log(STATUS_EMAIL_LEVEL, msg, *args, **kwargs)

###  Global cache to avoid reattaching handlers and adapters
_logger_cache = {}

def get_logger(name=__name__, context=None, run_env=None) -> logging.Logger:
    global _logger_cache

    """
    Returns a logger with contextual support using `ContextLoggerAdapter`.

    Args:
        name (str, optional): Name of the logger. Defaults to None (root logger).

    Returns:
        ContextLoggerAdapter: Logger wrapped with context support.
    """

    # Load the appropriate .env file if run_env is provided
    if run_env:
        try:
            dotenv_path = resolve_dotenv_path(run_env)
            load_dotenv(dotenv_path)
        except FileNotFoundError as e:
            print(f"⚠️ {e}")
    else:
        load_dotenv()  # fallback to default .env in cwd

    ### Generate a unique cache key for name + context combo
    cache_key = f"{name}-{str(context)}"
    if cache_key in _logger_cache:
        return _logger_cache[cache_key]  # Return cached adapter if exists

    logger = logging.getLogger(name)

    if not logger.handlers:  # Only add handlers once

        # === Config Setup ===
        log_dir = Path(__file__).resolve().parents[2] / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file_path = log_dir / "app.log"

        env = os.getenv("ENV", "DEV").upper()
        log_level = os.getenv("LOG_LEVEL", "DEBUG" if env == "DEV" else "INFO").upper()
        logger.setLevel(log_level)

        # === Formatters ===
        base_format = "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"
        date_format = "%Y-%m-%d %H:%M:%S"

        # Uncomment for .env info loaded.
        #print(f"env == {env}")
        #print("EMAIL_ALERTS =", os.getenv("EMAIL_ALERTS"))

        if env == "DEV":
            if has_rich:
                console_handler = RichHandler(rich_tracebacks=True, show_time=True, markup=True)
                console_formatter = logging.Formatter("%(message)s")
            elif ColoredFormatter:
                console_formatter = ColoredFormatter(
                    "%(log_color)s" + base_format,
                    datefmt=date_format,
                    log_colors={
                        'DEBUG': 'cyan',
                        'INFO': 'green',
                        'WARNING': 'yellow',
                        'ERROR': 'red',
                        'CRITICAL': 'bold_red',
                    },
                    )
                console_handler = logging.StreamHandler(sys.stdout)
                console_handler.setFormatter(console_formatter)
            else:
                console_formatter = ColorFormatter(base_format, datefmt=date_format)
                console_handler = logging.StreamHandler(sys.stdout)
                console_handler.setFormatter(console_formatter)
        elif env == "PROD" and jsonlogger:
            console_formatter = jsonlogger.JsonFormatter()
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(console_formatter)
        else:
            console_formatter = logging.Formatter(base_format, datefmt=date_format)
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(console_formatter)

        logger.addHandler(console_handler)

        # === File Handler (Rotating Monthly) ===
        file_handler = TimedRotatingFileHandler(
            filename=log_file_path,
            when="midnight",
            interval=30,
            backupCount=12,
            encoding='utf-8'
        )
        file_formatter = logging.Formatter(base_format, datefmt=date_format)
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        # === Email Alerts on Critical Errors ===
        if os.getenv("EMAIL_ALERTS", "false").lower() == "true":
            class NoAuthSMTPHandler(logging.Handler):
                def emit(self, record):
                    try:
                        ssl_context = ssl.create_default_context()

                        email_host = os.getenv("SMTP_HOST", "localhost")
                        email_port = int(os.getenv("SMTP_PORT", 465))
                        email_from = os.getenv("LOG_FROM_EMAIL", "log@yourapp.com")
                        email_to = os.getenv("LOG_ADMIN_EMAIL", "admin@yourapp.com")
                        subject = f"App Alert: {record.levelname}"

                        message = self.format(record)
                        msg = MIMEMultipart()
                        msg["From"] = email_from
                        msg["To"] = email_to
                        msg["Subject"] = subject
                        msg.attach(MIMEText(message, "plain"))

                        email_to_list = email_to.split(",")
                        with smtplib.SMTP_SSL(email_host, email_port, context=ssl_context) as server: # Pass ssl_context here
                            server.sendmail(email_from, email_to_list, msg.as_string())
                    except Exception:
                        self.handleError(record)

            email_handler = NoAuthSMTPHandler()
            email_handler.setLevel(min(STATUS_EMAIL_LEVEL, logging.CRITICAL))
            email_handler.setFormatter(file_formatter)
            class StatusEmailFilter(logging.Filter):
                def filter(self, record):
                    return record.levelno >= STATUS_EMAIL_LEVEL

            email_handler.addFilter(StatusEmailFilter())
            logger.addHandler(email_handler)

        logger.propagate = False

    # === Auto-populate context ===
    log_context = {}  # Create a separate dictionary for logging context
    if isinstance(context, dict):  # Check if context is a dictionary
        log_context.update(context) #if context is provided, update the log_context
    elif context:
        logger.warning(f"Expected a dictionary for 'context', got {type(context)}.  Will not update log_context with it.")

    log_context.setdefault("env", os.getenv("ENV", "DEV"))
    log_context.setdefault("program", os.path.basename(sys.argv[0]))

    ### [ADDED] Build and cache the context-aware logger
    adapter = ContextLoggerAdapter(logger, log_context)  # Pass log_context
    _logger_cache[cache_key] = adapter  # [ADDED] Store in cache

    # Uncomment to view handlers attached to a given logger.
    #print(f"[LOGGER DEBUG] Handlers attached to logger '{name}':")
    #for handler in logger.handlers:
    #    print(f"  - {type(handler).__name__} at level {logging.getLevelName(handler.level)}")


    return adapter

# ================ LOG_THIS Decorator below!

import functools
import logging
import time
import traceback
import inspect

SENSITIVE_KEYS = {"password", "pass", "token", "api_key", "secret", "access_token"}

def mask_value(k, v):
    """Return masked value if key is sensitive."""
    if k.lower() in SENSITIVE_KEYS:
        return "***"
    return v

def format_args(func, args, kwargs):
    """Map args to parameter names and apply masking."""
    sig = inspect.signature(func)
    bound = sig.bind_partial(*args, **kwargs)
    bound.apply_defaults()

    masked = {
        k: mask_value(k, v)
        for k, v in bound.arguments.items()
    }
    return masked

def log_this(
        logger,
        *,
        log_args=True,
        log_return=True,
        log_exceptions=True,
        log_duration=True,
        level="INFO"
):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            log_fn = getattr(logger, level.lower(), logger.info)

            if log_args:
                safe_args = format_args(func, args, kwargs)
                log_fn(f"➡️ Entering `{func.__name__}` with args={safe_args}")

            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time

                if log_return:
                    log_fn(f"✅ `{func.__name__}` returned: {result}")
                if log_duration:
                    log_fn(f"⏱️ `{func.__name__}` executed in {duration:.3f}s")

                return result
            except Exception as e:
                if log_exceptions:
                    logger.error(f"❌ Exception in `{func.__name__}`: {e}")
                    logger.error(traceback.format_exc())
                raise
        return wrapper
    return decorator

# ============================================= ON CRASHES decorator
def alert_on_crash(_func=None, *, logger=None):
    """
    Decorator to log function start, finish, and unhandled exceptions.
    If logger is not provided, uses get_logger(__name__).
    Can be used as @alert_on_crash or @alert_on_crash(logger=...).
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            local_logger = logger  # Don't mutate outer scope directly
            if local_logger is None:
                try:
                    from db_utils.logger import get_logger
                    local_logger = get_logger(func.__module__)
                except Exception as import_err:
                    print(f"[alert_on_crash] Failed to get logger: {import_err}")
                    local_logger = None

            try:
                if local_logger:
                    local_logger.info(f"▶️ Starting `{func.__name__}`")
                result = func(*args, **kwargs)
                if local_logger:
                    local_logger.info(f"✅ Finished `{func.__name__}`")
                    local_logger.status_email(f"📧 Finished '{func.__name__}'")
                return result
            except Exception as e:
                if local_logger:
                    local_logger.critical(
                        f"🔥 Whoa! Unhandled exception in `{func.__name__}`: {e}",
                        exc_info=True,
                    )
                else:
                    print(f"[alert_on_crash] Exception in {func.__name__}: {e}")
                raise
        return wrapper

    # Handle both @alert_on_crash and @alert_on_crash(logger=...)
    if callable(_func):
        return decorator(_func)
    return decorator
