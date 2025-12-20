from .header import header
from .left_drawer import left_drawer
from .conf_explorer import conf_explorer


import warnings

warnings.warn(
    "The components here are old and not officially supported. "
    "You should not use them. Instead use tgzr.nice package.",
    category=FutureWarning,
)
