# page_utils/page_utils.py

from functools import wraps
from typing import Dict, List, Callable, Any, Optional

import streamlit as st

from config import PageModel, AppConfig

from .page_registry import PageRegistry


def _make_page_callable(
    func: Callable[[Any], None], config: AppConfig, name: str
) -> Callable:
    """
    Creates a uniquely named callable function by injecting config.

    Args:
        func (Callable): The page-generating function.
        config (AppConfig): The application configuration object.
        name (str): The unique name for the callable.

    Returns:
        Callable: A callable function with injected config.
    """

    @wraps(func)
    def page_callable():
        func(config)

    page_callable.__name__ = name
    return page_callable


def _build_page(page_model: PageModel, config: AppConfig) -> st.Page:
    """
    Build a Streamlit Page object from a PageModel and config.

    Args:
        page_model (PageModel): A model containing page info.
        config (AppConfig): The application configuration object.

    Returns:
        st.Page: The constructed Streamlit Page.
    """

    # Create a uniquely named callable function
    callable_func = _make_page_callable(
        func=page_model.gen_func,
        config=config,
        name=page_model.title.replace(" ", "_").lower(),
    )

    return st.Page(
        page=callable_func,
        title=page_model.title,
        icon=page_model.icon,
        default=page_model.default,
        url_path=page_model.title,
    )


def create_navigation_mapping(config: AppConfig) -> Dict[str, List[st.Page]]:
    """
    Transform the registered PageModel instances into a mapping
    that Streamlit's navigation can use, keyed by section.

    Args:
        config (AppConfig): The configuration object.

    Returns:
        Dict[str, List[st.Page]]: A dictionary mapping section names
                                  to lists of st.Page objects.

    Raises:
        ValueError: If duplicate page titles are detected within the same section.
    """

    registered_page_models: List[PageModel] = PageRegistry.get_pages()
    pages_by_section: Dict[str, List[st.Page]] = {}

    for page_model in registered_page_models:
        section = page_model.section
        if section not in pages_by_section:
            pages_by_section[section] = []

        # Check for duplicate titles within the same section
        if any(
            existing_page.title == page_model.title
            for existing_page in pages_by_section[section]
        ):
            raise ValueError(
                f"Duplicate page title '{page_model.title}' in section '{section}'. "
                "Titles must be unique within a section."
            )

        st_page = _build_page(page_model, config)
        PageRegistry.store_page_object(page_model.title, st_page)

        pages_by_section[section].append(st_page)

    return pages_by_section


def fetch_page_by_title(page_title: str) -> Optional[st.Page]:
    """
    Fetch a single Streamlit Page object by its title.

    Args:
        config (AppConfig): The application configuration object.
        page_title (str): The title of the page to fetch.

    Returns:
        Optional[st.Page]: The corresponding Streamlit Page object if found, else None.

    Raises:
        ValueError: If duplicate page titles are detected or if the page title is not found.
    """

    pages_info: List[PageModel] = PageRegistry.get_pages()
    matching_pages = [page for page in pages_info if page.title == page_title]

    if not matching_pages:
        raise ValueError(f"No page found with the title '{page_title}'.")

    if len(matching_pages) > 1:
        raise ValueError(
            f"Multiple pages found with the title '{page_title}'. Titles must be unique."
        )

    page = PageRegistry.get_page_object(page_title)
    if page is None:
        raise ValueError(
            f"Page object for title '{page_title}' was not built or not stored in registry."
        )

    return page


def register_page(section: str, title: str, icon: str, default: bool = False):
    """
    A decorator to register a page with the given section, title, and icon.

    Args:
        section (str): The section under which the page should be registered.
        title (str): The title of the page.
        icon (str): The icon associated with the page.
        default (bool, optional): Whether this page should be the default page. Defaults to False.

    Returns:
        Callable: A decorator that registers the page and returns the original function.
    """

    def decorator(func: Callable[[Any], None]):
        page = PageModel(
            section=section, title=title, icon=icon, gen_func=func, default=default
        )
        PageRegistry.register(page)

        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper

    return decorator
