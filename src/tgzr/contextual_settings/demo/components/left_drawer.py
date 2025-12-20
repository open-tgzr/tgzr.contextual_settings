from nicegui import ui


async def left_drawer():
    with ui.left_drawer(elevated=True):  # .props("mini"):
        with ui.expansion("Examples", icon="history_edu", value=True).classes("border"):
            with ui.link(target="/examples/app_config"):
                ui.button("App Config", icon="app_shortcut")
            with ui.link(target="/examples/startpage"):
                ui.button("Start Page", icon="house")
            with ui.link(target="/examples/items_and_collections"):
                ui.button("Items & Collections", icon="collections_bookmark")
            with ui.link(target="/examples/cg_asset_task"):
                ui.button("CG Asset Task", icon="theaters")
            with ui.link(target="/examples/stresstest"):
                ui.button("Stress Test", icon="sick")

        with ui.expansion("Links & Doc", icon="auto_stories", value=True).classes(
            "border"
        ):
            ui.link("Readme", target="/readme")
            ui.link(
                "PyPI",
                target="https://pypi.org/project/tgzr.contextual_settings",
                new_tab=True,
            )
            ui.link(
                "Github",
                target="https://github.com/open-tgzr/tgzr.contextual_settings",
                new_tab=True,
            )
