from nicegui import ui
from ....components import header, left_drawer, conf_explorer

from tgzr.contextual_settings.stores.memory_store import MemoryStore
from .models import StartPageConf, StartPageTab, StartPageLink
from .components import SearchInput, FlagSvgs, start_page
from ....components.conf_explorer import ContextNameChips

_STORE = None


def get_store() -> MemoryStore:
    global _STORE
    if _STORE is None:
        _STORE = _create_store()
    return _STORE


TAILWIND_SVG_LOGO = """
<svg viewBox="0 0 32 21" fill="none">
<path class="fill-sky-400" d="M17.183 0C12.6 0 9.737 2.291 8.59 6.873c1.719-2.29 3.723-3.15 6.014-2.577 1.307.326 2.242 1.274 3.275 2.324 1.685 1.71 3.635 3.689 7.894 3.689 4.582 0 7.445-2.291 8.591-6.872-1.718 2.29-3.723 3.15-6.013 2.576-1.308-.326-2.243-1.274-3.276-2.324C23.39 1.98 21.44 0 17.183 0ZM8.59 10.309C4.01 10.309 1.145 12.6 0 17.182c1.718-2.291 3.723-3.15 6.013-2.577 1.308.326 2.243 1.274 3.276 2.324 1.685 1.71 3.635 3.689 7.894 3.689 4.582 0 7.445-2.29 8.59-6.872-1.718 2.29-3.722 3.15-6.013 2.577-1.307-.327-2.242-1.276-3.276-2.325-1.684-1.71-3.634-3.689-7.893-3.689Z">
</path>
</svg>
"""


def _create_store() -> MemoryStore:
    store = MemoryStore()

    conf = StartPageConf(title="My Start Page", icon=None)
    conf.show_search = True

    # DEFAULT CONTEXT
    store.set_context_info(
        "default",
        color="#FFFA77",
        icon="settings_suggest",
        description="Sets search visible and adds Bookmark and News tabs with their links",
    )

    conf.tab_names = ["bookmarks", "news"]
    store.update_context("default", conf)

    bookmarks_tab = StartPageTab(title="Bookmarks", icon="bookmark")
    store.update_context("default", bookmarks_tab, "tabs.bookmarks")
    news_tab = StartPageTab(title="News", icon="newspaper")
    store.update_context("default", news_tab, "tabs.news")

    bookmark_items = [
        StartPageLink(
            title="NiceGUI Doc",
            color="#76aadb",
            url="https://nicegui.io/documentation",
            icon=None,
            image_url="https://daelonsuzuka.gallerycdn.vsassets.io/extensions/daelonsuzuka/nicegui/0.9.6/1750904459388/Microsoft.VisualStudio.Services.Icons.Default",
        ),
        StartPageLink(
            title="Material Icons",
            icon="offline_bolt",
            color="deep-orange-6",
            url="https://fonts.google.com/icons?icon.set=Material+Icons",
        ),
        StartPageLink(
            title="Eva Icons",
            icon="eva-heart-outline",
            color="pink-4",
            url="https://akveo.github.io/eva-icons/#/",
        ),
        StartPageLink(
            title="Tailwind Doc",
            icon=None,
            image_svg=TAILWIND_SVG_LOGO,
            color="grey-5",
            url="https://tailwindcss.com/brand",
        ),
    ]
    for bi in bookmark_items:
        ID = bi.title.lower().replace(".", "_").replace(" ", "_")
        store.append("default", "tabs.bookmarks.item_names", ID)
        store.update_context("default", bi, f"tabs.bookmarks.{ID}")

    for name in ["France", "United Kingdom", "Canada", "Japan", "South Korea"]:
        ID = f"news_{name.lower()}"
        link = StartPageLink(
            title=name,
            icon=None,
            url=f"https://news.google.com/search?q={name}",
            image_url=FlagSvgs.svg_url(name),
        )
        store.append("default", "tabs.news.item_names", ID)
        store.update_context("default", link, f"tabs.news.{ID}")

    # PERSONAL CONTEXT
    store.set_context_info(
        "personal",
        color="#FFA277",
        icon="edit_attributes",
        description="Changes the title, title font and color of News/France",
    )

    store.set("personal", "title_font_family", "Ballet")
    store.set("personal", "title", "Dee Home")
    store.set("personal", "tabs.news.news_france.color", "yellow")

    # TEAM CONTEXT
    store.set_context_info(
        "team",
        color="#77FDFF",
        icon="business",
        description="Adds the team tab and links for members.",
    )

    team_tab = StartPageTab(title="Team", icon="group")
    store.update_context("team", team_tab, "tabs.team")
    store.append("team", "tab_names", "team")

    for name in ["Alice", "Bob", "Carole", "Dee", "Eve"]:
        ID = f"team_{name.lower()}"
        url = f"https://robohash.org/{name.lower()}.png"
        link = StartPageLink(
            title=name,
            icon=None,
            url=url,
            image_url=url,
        )
        store.append("team", "tabs.team.item_names", ID)
        store.update_context("team", link, f"tabs.team.{ID}")

    # store.env_override("ENV", "app.service.host", "MYAPP_HOST")
    # store.env_override("ENV", "app.service.port", "MYAPP_PORT")

    # store.set_context_info("defaults", color="#9996C3", icon="input")
    # store.set_context_info("DEV", color="#FFA600", icon="data_object")
    # store.set_context_info("ENV", color="#88EDFF", icon="attach_money")

    return store


@ui.page("/examples/startpage")
async def app_config_example():
    await header()
    await left_drawer()

    # load eva icons and ballet google font:
    ui.add_head_html(
        """
    <link href="https://unpkg.com/eva-icons@1.1.3/style/eva-icons.css" rel="stylesheet" />
    <link href='https://fonts.googleapis.com/css?family=Ballet' rel='stylesheet'>
    """
    )

    with ui.tabs().classes("w-full") as tabs:
        demo = ui.tab("Start Page")
        explorer = ui.tab("Conf")

    with ui.tab_panels(tabs, value=demo).classes("w-full"):
        with ui.tab_panel(demo):
            CONTEXT = ["default", "personal", "team"]

            def on_context_name_changed(names):
                CONTEXT[:] = names
                render_start_page.refresh()

            store = get_store()
            context_chips = ContextNameChips(
                store,
                on_context_names_changed=on_context_name_changed,
                hide_add_input=True,
            )
            for name in CONTEXT:
                context_chips.add_context_name(name)

            ui.markdown(
                "Toggle / Reorder contexts here 👆\n\nto change configuration for the page there 👇.\n\n"
                'Go to the "Settings" tab to explore the tgzr.contextual_settings data'
            )

            @ui.refreshable
            async def render_start_page():
                await start_page(get_store(), CONTEXT)

            await render_start_page()
        with ui.tab_panel(explorer):
            await conf_explorer(get_store())
