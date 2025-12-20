"""
Yep, I'm using tgzr.contextual_settings to configure tgzr.contextual_settings' demo.
So meta ^_^

"""

import pydantic

from tgzr.contextual_settings.stores.memory_store import MemoryStore

store = MemoryStore()

store.set("defaults", "host", "0.0.0.0")
store.set("defaults", "port", 8080)
store.set("defaults", "reload", True)
store.set("defaults", "title", "TGZR Contextual Settings")

store.env_override("ENV", "host", "CONTEXTSETTINGS_ENV")
store.env_override("ENV", "port", "CONTEXTSETTINGS_PORT")
store.env_override("ENV", "reload", "CONTEXTSETTINGS_RELOAD")
store.env_override("ENV", "title", "CONTEXTSETTINGS_TITLE")


class Config(pydantic.BaseModel):
    host: str = "localhost"
    port: int = 8888
    reload: bool = False
    title: str = "Contextual Setting"


def get_config():
    config = store.get_context(["defaults", "ENV"], Config)
    return config
