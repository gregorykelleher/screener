# config/config_models.py

from typing import Any, List, Dict, Callable
from pydantic import BaseModel

class TableModel(BaseModel):
    title: str
    icon: str
    columns_to_hide: List[str]
    fetch_func: Callable[[Any], List[Dict]]
    columns_mapping: Dict[str, str]
    default_sort: List[Dict[str, str]]

class MainConfig(BaseModel):
    overview_config: List[TableModel]
