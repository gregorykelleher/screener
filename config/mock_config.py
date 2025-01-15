# config/mock_config.py

from config.config_models import MainConfig
from tests.mocks.mock_dashboards.mock_overview.mock_overview_config import overview_config

config = MainConfig(
    overview_config=overview_config
)
