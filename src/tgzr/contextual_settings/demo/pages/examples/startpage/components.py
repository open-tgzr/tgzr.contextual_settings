from nicegui import ui
from nicegui.events import ValueChangeEventArguments

from .models import StartPageConf, StartPageTab, StartPageLink
from .....stores.base_store import BaseStore


class SearchInput(ui.input):
    def __init__(self) -> None:
        super().__init__(label="Search")
        self.props("clearable dense")
        self.classes("text-h5 m-2 pl-3 pr-3 shadow-md shadow-sky-200").style(
            "background-color: rgba(255,255,255,0.2);"
            "border-width: 1px;"
            "border-color: rgba(255,255,255,0.3);"
            "border-radius: 16px;"
        )
        self.on("keydown.enter", self._apply)

    def _apply(self):
        ui.notify(
            "Sorry, this is just a demo ¯\\_(ツ)_/¯",
            position="center",
            close_button=True,
        )


class FlagSvgs:
    france = "https://flagicons.lipis.dev/flags/1x1/fr.svg"
    united_kingdom = "https://flagicons.lipis.dev/flags/1x1/gb.svg"
    canada = "https://flagicons.lipis.dev/flags/1x1/ca.svg"
    japan = "https://flagicons.lipis.dev/flags/1x1/jp.svg"
    south_korea = "https://flagicons.lipis.dev/flags/1x1/kr.svg"

    @classmethod
    def svg_url(cls, name: str):
        name = name.lower().replace(" ", "_")
        return getattr(cls, name)


async def start_page(store: BaseStore, context):
    conf = store.get_context(context, StartPageConf)

    with ui.element("div").classes("w-full") as e:

        bg_url = "/assets/sky_bg.jpg"
        e.style(f"background-image: url('{bg_url}')")
        with ui.column().classes("gap-0"):

            # ui.space()
            with ui.row(align_items="center").classes("w-full mt-5"):
                ui.space()
                if conf.icon is not None:
                    ui.icon(conf.icon).classes("text-5xl")
                lb = ui.label(conf.title).classes("text-6xl mt-5")
                if conf.title_font_family is not None:
                    lb.style(f'font-family: "{conf.title_font_family}";')
                ui.space()

            if conf.show_search:
                with ui.row().classes("w-full mt-5"):
                    ui.space()
                    SearchInput()
                    ui.space()

            ui.space()
            default_tab = conf.tab_names and conf.tab_names[-1] or None
            with ui.row().classes("w-full"):
                with ui.tabs().classes("w-full").props("indicator-color=white") as tabs:
                    tabs.value = default_tab
                    for tab_name in conf.tab_names:
                        tab = store.get_context(
                            context, StartPageTab, f"tabs.{tab_name}"
                        )
                        ui.tab(tab_name, label=tab.title, icon=tab.icon).props(
                            "no-caps "
                        )
            with ui.row().classes("w-full"):

                ui.space()
                with ui.carousel(
                    animated=True,
                    arrows=True,
                    navigation=False,
                    on_value_change=lambda: ui.query(".nicegui-carousel-slide").style(
                        "padding:0px;"
                    ),
                ).classes("w-3/4 mb-5 bg-transparent") as carousel:
                    tabs.bind_value(carousel, "value")
                    carousel.props(
                        "transition-prev=jump-right transition-next=jump-left"
                    )
                    for tab_name in conf.tab_names:
                        with ui.carousel_slide(name=tab_name) as cs:
                            tab = store.get_context(
                                context, StartPageTab, f"tabs.{tab_name}"
                            )
                            with (
                                ui.element("div")
                                .classes("w-full h-[50vh] p-5")
                                .style(
                                    "background-color: rgba(255,255,255,0.2);"
                                    "border-width: 1px;"
                                    "border-color: rgba(255,255,255,0.3);"
                                    "border-radius: 16px;"
                                )
                            ):
                                with ui.row().classes("m-5 gap-10"):
                                    for item_name in tab.item_names:
                                        item = store.get_context(
                                            context,
                                            StartPageLink,
                                            f"tabs.{tab_name}.{item_name}",
                                        )
                                        with ui.link(
                                            target=item.url, new_tab=True
                                        ).classes("!no-underline"):
                                            with ui.column(align_items="center"):
                                                with (
                                                    ui.button(
                                                        icon=item.icon,
                                                        color=item.color or "blue-3",
                                                    )
                                                    .props("round stack glossy")
                                                    .classes("text-2xl")
                                                ):
                                                    if item.image_url is not None:
                                                        ui.image(
                                                            item.image_url
                                                        ).classes("w-14 rounded-full")
                                                    elif item.image_svg:
                                                        ui.interactive_image(
                                                            size=(10, 10),
                                                            cross=False,
                                                            content=item.image_svg,
                                                        ).classes("w-10")
                                                ui.label(item.title)
                ui.space()
