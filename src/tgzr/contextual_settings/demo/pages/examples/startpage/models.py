from typing import Annotated, Any

import pydantic


class StartPageLink(pydantic.BaseModel):
    title: str = "Link"
    icon: str | None = "link"
    url: str = "https://example.com"
    image_url: str | None = None
    image_svg: str | None = None
    color: str | None = None


class StartPageTab(pydantic.BaseModel):
    title: str = "Tab"
    icon: str = "tab"
    item_names: Annotated[
        list[str],
        dict(names_for="items", getter="get_item"),
    ] = []

    # items: list[tuple[str, StartPageLink]] = []


class StartPageConf(pydantic.BaseModel):
    title: str = "My Start Page"
    title_font_family: str | None = None
    icon: str | None = None
    show_search: bool = False
    tab_names: Annotated[
        list[str],
        dict(names_for="tabs", getter="get_tab"),
    ] = []

    # tabs: list[tuple[str, StartPageTab]] = []

    # def get_tab(self, tab_name: str) -> StartPageTab | None:
    #     print("---->", self.tabs)
    #     return None  # dict(self.tabs).get(tab_name)
