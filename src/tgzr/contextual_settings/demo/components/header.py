from nicegui import ui


async def header():
    with ui.header(elevated=True):
        ui.label("Contextual Settings").classes("text-h3 absolute-center")
        ui.image("/assets/logo.svg").classes("w-10 h-10")
