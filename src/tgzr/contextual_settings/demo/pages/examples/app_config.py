from nicegui import ui
from ...components import header, left_drawer, conf_explorer

from tgzr.contextual_settings.stores.memory_store import MemoryStore

_STORE = None


def get_store() -> MemoryStore:
    global _STORE
    if _STORE is None:
        _STORE = _create_store()
    return _STORE


def _create_store() -> MemoryStore:
    store = MemoryStore()
    store.set("defaults", "app.title", "MyApp")
    store.set("defaults", "app.version", "1.0.0")
    store.set("defaults", "app.service.host", "0.0.0.0")
    store.set("defaults", "app.service.port", 8080)

    store.set("DEV", "app.title", "MyApp[dev]")
    store.set("DEV", "app.version", "1.0.0+devXXX")
    store.set("DEV", "app.service.host", "localhost")

    store.env_override("ENV", "app.service.host", "MYAPP_HOST")
    store.env_override("ENV", "app.service.port", "MYAPP_PORT")

    store.set_context_info("defaults", color="#9996C3", icon="input")
    store.set_context_info("DEV", color="#FFA600", icon="data_object")
    store.set_context_info("ENV", color="#88EDFF", icon="attach_money")

    return store


@ui.page("/examples/app_config")
async def app_config_example():
    await header()
    await left_drawer()
    await conf_explorer(get_store())
