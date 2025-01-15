# config/main_config.py

from config.config_models import MainConfig
from src.dashboards.overview.overview_config import overview_config

config = MainConfig(
    overview_config=overview_config
)
