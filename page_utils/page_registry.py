# page_registry.py

from typing import List, Optional, Dict
import streamlit as st

from config import PageModel


class PageRegistry:
    _pages: List[PageModel] = []
    _default_page_title: Optional[str] = None
    _page_objects: Dict[str, st.Page] = {}

    @classmethod
    def register(cls, page: PageModel):
        if page.default and cls._default_page_title is not None:
            raise ValueError(
                f"Cannot set '{page.title}' as the default page because "
                f"'{cls._default_page_title}' is already the default."
            )

        if page.default:
            cls._default_page_title = page.title

        cls._pages.append(page)

    @classmethod
    def get_pages(cls) -> List[PageModel]:
        return cls._pages

    # store built st.Page object
    @classmethod
    def store_page_object(cls, title: str, page_obj: st.Page):
        cls._page_objects[title] = page_obj

    # retrieve a previously stored st.Page object
    @classmethod
    def get_page_object(cls, title: str) -> Optional[st.Page]:
        return cls._page_objects.get(title)
