# config/config_models.py

from typing import Callable, Dict, List, Any, Optional
from pydantic import BaseModel


class TableModel(BaseModel):
    title: str
    icon: str
    columns_to_hide: List[str]
    fetch_func: Callable[[Any], List[Dict]]
    columns_mapping: Dict[str, str]
    default_sort: List[Dict[str, str]]


class PageModel(BaseModel):
    section: str
    title: str
    icon: str
    gen_func: Callable[[Any], None]
    default: Optional[bool] = False


class AppConfig(BaseModel):
    overview_config: List[TableModel]
    movers_config: List[TableModel]
