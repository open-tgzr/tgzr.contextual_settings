from __future__ import annotations

from typing import Any
from types import SimpleNamespace
import functools


class ContextData(SimpleNamespace):
    # FIXME: ensure key is never named like one of these methods!

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ContextData:
        if not isinstance(d, dict):
            return d

        # Recursively convert nested dictionaries
        converted = {}
        for key, value in d.items():
            if isinstance(value, dict):
                converted[key] = ContextData.from_dict(value)
            else:
                converted[key] = value
        return ContextData(**converted)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def dot_set(self, dotted_key: str, value: Any) -> None:
        if "." not in dotted_key:
            target_context_data = self
            attr_to_set = dotted_key
        else:
            # names = self.escaped_split(dotted_key, ".")
            names = dotted_key.split(".")
            attr_to_set = names.pop(-1)

            def get_or_create(o, name):
                try:
                    return getattr(o, name)
                except AttributeError:
                    sub = self.__class__()
                    setattr(o, name, sub)
                    return sub

            target_context_data = functools.reduce(get_or_create, names, self)

        setattr(target_context_data, attr_to_set, value)

    def dot_get(self, dotted_key: str) -> Any:
        # This would have been amazing:
        # return operator.attrgetter(dotted_key)(self)
        # but it does not support default value :'/
        if "." not in dotted_key:
            return getattr(self, dotted_key, self.__class__())
        else:
            # names = self.escaped_split(dotted_key, ".")
            names = dotted_key.split(".")

            def get_or_create(o, name):
                try:
                    return getattr(o, name)
                except AttributeError:
                    sub = self.__class__()
                    setattr(o, name, sub)
                    return sub

            return functools.reduce(get_or_create, names, self)

    def to_dict(self) -> dict[str, Any]:
        return self._obj_to_dict(self)

    @classmethod
    def _obj_to_dict(cls, obj) -> Any:
        if isinstance(obj, SimpleNamespace):
            return {k: cls._obj_to_dict(v) for k, v in vars(obj).items()}
        elif isinstance(obj, list):
            return [cls._obj_to_dict(item) for item in obj]
        elif isinstance(obj, tuple):
            return tuple(cls._obj_to_dict(item) for item in obj)
        else:
            return obj
