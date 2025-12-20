from typing import Callable
import os
import string

_ENVIRON_GETTER: Callable[[], dict[str, str]] = lambda: os.environ  # type: ignore azy s'pareil gro fepayesh.


def get_environ() -> dict[str, str]:
    return _ENVIRON_GETTER()


def set_environ_getter(getter) -> None:
    """
    This is for testing/demo purpose.
    It allow to use a custom dict instead
    of os.environ
    """
    global _ENVIRON_GETTER
    _ENVIRON_GETTER = getter


def reset_environ_getter() -> None:
    global _ENVIRON_GETTER
    _ENVIRON_GETTER = lambda: os.environ  # type: ignore vous chipottez mon cher...


def _expand_vars(text: str, vars: dict[str, str]) -> str:
    max = 100
    curr = 0
    while "$" in text:
        curr += 1
        if curr > max:
            return text

        last = text
        try:
            text = string.Template(text).substitute(vars)
        except KeyError as err:
            # FIXME: this is redondant with `if text == last`
            print(" !", text, "->", err)
            return text

        if text == last:
            # no substitution done, job is over:
            # (this can happen with '$' in text when
            # the env var does not exists)
            return text
    return text


def _expand_path(text: str) -> list[str]:
    if text.startswith("[") and text.endswith("]"):
        path = text[1:-1]
        names = [n for n in path.split("/") if n]  # also remove empty names!
        return ["/".join(names[:i]) for i in range(1, len(names) + 1)]
    return [text]


def expand_context_name(name: str, vars: dict[str, str]) -> list[str]:
    name = _expand_vars(name, vars)
    names = _expand_path(name)
    return names


def expand_context_names(context_names: list[str]) -> list[str]:
    """
    Expand each name in context_names and return the resulting context
    names.

    Each name can contain envvar reference:
        `"prod:$PRODNAME"` -> "prod"+os.environ['PRODNAME']
    Each name is an expandable path if its is enclosed with square brackets:
        `[path/to/entity]` -> ['path', 'path/to', 'path/to/entity']

    Envvars value can contain envvar reference.

    Envvars are resolved *before* path expansion, so you can do:
        `"[$PROJ/shots/$SHOT/tasks/$TASK]"`
    or even
        `"[$ENTITY_PATH]"`

    When a context_name resolve to something containing an already
    used context_name, this new one is discarded.
    i.e: Only the first occurrence of a context name is kept.
    """
    vars = get_environ()
    final_context_names = []
    for name in context_names:
        names = expand_context_name(name, vars)
        final_context_names.extend([n for n in names if n not in final_context_names])
    return final_context_names
