# equity-aggregator/logging_config.py

import logging.config
import os
from datetime import date

# get log directory or default
LOG_DIR = os.getenv("LOG_DIR", "./data/logs")
os.makedirs(LOG_DIR, exist_ok=True)

# construct log file path
LOG_FILE = os.path.join(LOG_DIR, f"equity_aggregator_{date.today():%Y-%m-%d}.log")

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
            "filename": LOG_FILE,
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
