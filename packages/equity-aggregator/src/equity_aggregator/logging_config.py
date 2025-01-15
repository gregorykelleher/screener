# equity-aggregator/logging_config.py

import logging.config
import os

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s | %(module)-20s | %(levelname)-5s | %(message)s",
            "datefmt": "%Y-%m-%dT%H:%M:%S",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "DEBUG",
            "formatter": "standard",
            "stream": "ext://sys.stdout",
        },
    },
    "loggers": {
        "equity_aggregator": {
            "level": "INFO",
            "handlers": ["console"],
            "propagate": False,
        },
    },
    "root": {
        "level": "WARNING",
        "handlers": ["console"],
    },
}


def configure_logging() -> None:
    """
    Configure logging for the application based on the LOG_CONFIG environment variable.

    Copies the base LOGGING configuration and sets the console handler's log level
    according to the environment:
        - 'production': sets log level to WARNING.
        - 'debug': sets log level to DEBUG.
        - Defaults to INFO for 'development' or if LOG_CONFIG is unset.

    Applies the final configuration using logging.config.dictConfig.

    Args:
        None

    Returns:
        None
    """
    config = LOGGING.copy()

    # Determine the log configuration (default to 'development')
    env = os.getenv("LOG_CONFIG", "development").lower()
    level_map = {
        "production": "WARNING",
        "debug": "DEBUG",
        "development": "INFO",
    }
    log_level_selected = level_map.get(env, "INFO")

    # set both root logger and console handler to that level
    config["loggers"]["equity_aggregator"]["level"] = log_level_selected
    config["handlers"]["console"]["level"] = log_level_selected

    logging.config.dictConfig(config)
