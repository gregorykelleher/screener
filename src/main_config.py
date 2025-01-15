# src/main_config.py

from config import AppConfig
from .pages import overview_config, movers_config

config = AppConfig(overview_config=overview_config, movers_config=movers_config)
