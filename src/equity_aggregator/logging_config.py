# equity-aggregator/logging_config.py

import logging.config
import os
from datetime import date

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "standard",
            "stream": "ext://sys.stdout",
        },
        "file": {
            "class": "logging.FileHandler",
            "level": "DEBUG",
            "formatter": "standard",
            "filename": None,  # set lazily at runtime using configure_logging()
            "encoding": "utf8",
        },
    },
    "formatters": {
        "standard": {
            "format": "%(asctime)s | %(module)-20s | %(levelname)-5s | %(message)s",
            "datefmt": "%Y-%m-%dT%H:%M:%S",
        },
    },
    "loggers": {
        "equity_aggregator": {
            "level": "INFO",
            "handlers": ["console", "file"],
            "propagate": False,
        },
    },
    "root": {
        "level": "WARNING",
        "handlers": ["console", "file"],
    },
}


def configure_logging() -> None:
    """
    Configures logging for the application based on the LOG_CONFIG environment variable.

    This function copies the base LOGGING configuration and adjusts console handler's
    log level according to the environment:
        - 'production': sets console log level to WARNING.
        - 'debug': sets console log level to DEBUG.
        - 'development' or unset: sets console log level to INFO.

    The file handler is always set to DEBUG level. The logger 'equity_aggregator' is set
    to DEBUG level.

    Args:
        None

    Returns:
        None
    """
    config = LOGGING.copy()

    # Resolve the log file path
    config["handlers"]["file"]["filename"] = _resolve_log_file()

    # Determine the log configuration (default to 'development')
    env = os.getenv("LOG_CONFIG", "development").lower()

    console_level = {
        "production": "WARNING",
        "debug": "DEBUG",
        "development": "INFO",
    }.get(env, "INFO")

    config["loggers"]["equity_aggregator"]["level"] = "DEBUG"

    # console handler always set to environment level
    config["handlers"]["console"]["level"] = console_level

    # file handler always set to DEBUG
    config["handlers"]["file"]["level"] = "DEBUG"

    logging.config.dictConfig(config)


def _resolve_log_file() -> str:
    """
    Resolves the log file path for the current date and ensures log directory exists.

    Args:
        None

    Returns:
        str: Absolute path to the log file for today's date.
    """
    log_dir = os.getenv("LOG_DIR", "./data/logs")
    os.makedirs(log_dir, exist_ok=True)
    return os.path.join(log_dir, f"equity_aggregator_{date.today():%Y-%m-%d}.log")
