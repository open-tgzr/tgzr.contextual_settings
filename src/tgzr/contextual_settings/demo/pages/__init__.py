import pathlib
from nicegui import ui

from ..components import header, left_drawer

from . import examples  # to declare pages


@ui.page("/")
async def index_page():
    await header()
    await left_drawer()
    with ui.row():
        ui.space()
        ui.markdown("Contextual Settings Demo")
        ui.space()


@ui.page("/readme")
async def readme_page():
    await header()
    await left_drawer()
    readme_file = (pathlib.Path(__file__) / ".." / "README.md").resolve()
    # print("????", readme_file)
    if not readme_file.exists():
        ui.markdown(
            f"""
            The README is only availabel when running the GUI from source...
            ({readme_file})

            Got to PyPI if you want to read it 👍
            """
        )
    else:
        with open(readme_file, "r") as fp:
            md_content = fp.read()
        ui.markdown(md_content)
