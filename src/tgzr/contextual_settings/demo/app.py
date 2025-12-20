from pathlib import Path

import fastapi
import nicegui

from tgzr.contextual_settings.ops import Op
from tgzr.contextual_settings import context_name

from . import pages

_CUSTOM_ENVIRON: dict[str, str] = {
    "MYAPP_HOST": "127.0.0.1",
    # "MYAPP_PORT": "8088",
    "PROJECT_CODE": "my_project_code",
}


def get_custom_environ() -> dict[str, str]:
    return _CUSTOM_ENVIRON


def create_app(title: str):
    # configure the environ getter to our custom one
    # to avoid messing up with the real environ in
    # the demo app.
    Op.set_environ_getter(get_custom_environ)
    context_name.set_environ_getter(get_custom_environ)

    app = fastapi.FastAPI(title=title)

    assets_path = Path(__file__) / ".." / "assets"
    assets_path = assets_path.resolve()
    nicegui.app.custom_env = _CUSTOM_ENVIRON  # type: ignore wallah j'le met quand meme azy tavu la...
    nicegui.app.add_media_files("/assets", assets_path)
    nicegui.app.add_static_file(
        url_path="/favicon.ico", local_file=assets_path / "favicon" / "favicon.ico"
    )
    nicegui.ui.run_with(
        app,
        title=title,  # Default title for pages without specific one
        storage_secret="storage_secret",  # Needed for session management
        favicon="/favicon.ico",
    )
    return app


app = create_app(title="Contextual Settings")
