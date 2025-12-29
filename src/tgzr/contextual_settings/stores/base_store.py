from typing import Any, TypeVar
import collections.abc
import time
import logging

import pydantic

from .. import ops
from ..context_name import expand_context_names, expand_context_name, get_environ
from ..context_data import ContextData

ModelType = TypeVar("ModelType", bound=pydantic.BaseModel)

logger = logging.getLogger(__name__)


class BaseStore:
    def __init__(self):
        pass

    def _append_op(self, context_name: str, op: ops.Op) -> None: ...

    def _get_ops(self, context_name) -> ops.OpBatch: ...

    def get_context_names(self) -> tuple[str, ...]: ...

    def set_context_info(self, context_name: str, **kwargs: Any) -> None: ...

    def get_context_info(self, context_name: str) -> dict[str, Any]: ...

    def expand_context_name(self, context_name: str) -> list[str]:
        """
        Return the context name with env var resolved and path expanded.
        See `tgzr.contextual_settings.context_names.expand_context_name()` for details.

        """
        envvars = get_environ()
        return expand_context_name(context_name, vars=envvars)

    def _resolve_context_data(
        self, contexts: list[str], with_history: bool = False
    ) -> ContextData:
        data = ContextData()
        context_info: dict[str, str | dict[str, str]] | None = None
        history_data = None
        if with_history:
            history_data = ContextData()
        for context_name in contexts:
            context_ops = self._get_ops(context_name)
            if with_history:
                context_info = dict(context_name=context_name)
            context_ops.render(
                data, history_data=history_data, context_info=context_info
            )
        if with_history:
            setattr(data, "__history__", history_data)
        return data

    def _resolve_flat(
        self, contexts: list[str], with_history: bool = False
    ) -> dict[str, Any]:
        data = {}
        context_info: dict[str, str | dict[str, str]] | None = None
        history_data = None
        if with_history:
            history_data = {}
        for context_name in contexts:
            context_ops = self._get_ops(context_name)
            if with_history:
                context_info = dict(context_name=context_name)
            context_ops.render_flat(
                data, history_data=history_data, context_info=context_info
            )
        if with_history:
            data["__history__"] = history_data
        return data

    def set(self, context_name: str, name: str, value: Any) -> None:
        self._append_op(context_name, ops.Set(name, value))

    def toggle(self, context_name: str, name: str) -> None:
        self._append_op(context_name, ops.Toggle(name))

    def add(self, context_name: str, name: str, value: Any) -> None:
        self._append_op(context_name, ops.Add(name, value))

    def sub(self, context_name: str, name: str, value: Any) -> None:
        self._append_op(context_name, ops.Sub(name, value))

    def set_item(
        self, context_name: str, name: str, index: int, item_value: Any
    ) -> None:
        self._append_op(context_name, ops.SetItem(name, index, item_value))

    def del_item(self, context_name: str, name: str, index: int) -> None:
        self._append_op(context_name, ops.DelItem(name, index))

    def remove(self, context_name: str, name: str, item: str) -> None:
        self._append_op(context_name, ops.Remove(name, item))

    def append(self, context_name: str, name: str, value: Any) -> None:
        self._append_op(context_name, ops.Append(name, value))

    def env_override(self, context_name: str, name: str, envvar_name: str) -> None:
        """Set the value from the given env var only if that env var exists."""
        self._append_op(context_name, ops.EnvOverride(name, envvar_name))

    def pop(self, context_name: str, name: str, index: int | slice) -> None:
        self._append_op(context_name, ops.Pop(name, index))

    def remove_slice(
        self,
        context_name: str,
        name: str,
        start: int,
        stop: int,
        step: int | None = None,
    ) -> None:
        self._append_op(context_name, ops.RemoveSlice(name, start, stop, step))

    def call(
        self,
        context_name: str,
        name: str,
        method_name: str,
        args: list[Any],
        kwargs: dict[str, Any],
    ) -> None:
        self._append_op(context_name, ops.Call(name, method_name, args, kwargs))

    def update_context_flat(
        self, context_name: str, flat_dict: dict[str, Any], path: str | None = None
    ) -> None:
        k_prefix = ""
        if path is not None:
            k_prefix = path + "."
        for k, v in flat_dict.items():
            self.set(context_name, k_prefix + k, v)

    def update_context_dict(
        self,
        context_name: str,
        deep_dict: dict[str, Any | dict[str, Any]],
        path: str | None = None,
    ) -> None:
        k_prefix = ""
        if path is not None:
            k_prefix = path + "."
        for k, v in deep_dict.items():
            if isinstance(v, collections.abc.Mapping):
                self.update_context_dict(context_name, v, k_prefix + k)  # type: ignore don't know how to deal with v annotation :/
            else:
                self.set(context_name, k_prefix + k, v)

    def update_context(
        self,
        context_name: str,
        model: ModelType,
        path: str | None = None,
        exclude_defaults: bool = True,
    ):
        """
        Update the given context with value from the given model.

        If `exclude_unset` is True, only non-default values will
        be stored.
        If you need to store a default value without storing all
        the default values, dump the model yourself keeping only
        the fields you want to store, and use `update_context_dict()`.
        """
        deep_dict = model.model_dump(exclude_defaults=exclude_defaults)
        self.update_context_dict(context_name, deep_dict, path)

    def get_context_flat(
        self,
        context: list[str],
        path: str | None = None,
        with_history: bool = False,
    ) -> dict[str, Any]:
        values = self._resolve_flat(expand_context_names(context), with_history)
        # FIXME: reduce string template in all flat values here!

        if path is not None:
            value = dict()
            k_prefix = path + "."
            for k, v in values.items():
                if k == path or k.startswith(k_prefix):
                    rk = k[len(k_prefix) :]  # TODO: should cutting prefix be optional ?
                    value[rk] = v
            if with_history:
                history = values["__history__"]
                value_history = dict()
                for k, v in history.items():
                    if k == path or k.startswith(k_prefix):
                        rk = k[
                            len(k_prefix) :
                        ]  # TODO: should cutting prefix be optional ?
                        value_history[rk] = v
                value["__history__"] = value_history
        else:
            value = values

        return value

    def get_context_dict(
        self,
        context: list[str],
        path: str | None = None,
        with_history: bool = False,
    ) -> dict[str, Any]:
        values = self._resolve_context_data(expand_context_names(context), with_history)
        # FIXME: reduce string template in all values here!

        if path is not None:
            value = values.dot_get(path)
            history_key = "__history__"
            if not isinstance(value, ContextData):
                # The path leads to a value, we need to build an empty parent ContextData:
                final_key = path.rsplit(".")[-1]
                history_key = f"__history__.{final_key}"
                cd = ContextData()
                cd.dot_set(final_key, value)
                value = cd

            if with_history:
                history = values.__history__.dot_get(path)
                value.dot_set(history_key, history)
        else:
            value = values

        if isinstance(value, ContextData):
            value = value.to_dict()

        return value

    def get_context(
        self,
        context: list[str],
        model_type: type[ModelType],
        path: str | None = None,
    ) -> ModelType:
        """
        Return the value for the given context.

        The `context` argument is a list of context name to apply.
        Each name can contain envvars and/or path to expand.
        See `context.expand_context_names()` doc for details.

        If `path` is not None, it is the dotted name of
        a value in the store.
        If `path` is None, the a value containing all values is returned.

        If `model_type` is provided, it must be a pydantic.BaseModel and
        the returned value will be an instance of it.
        This model *must* be have default for *all fields*
        (i.e `model_type()` must be valid)

        Note that the return value is validated against model_type and a
        pydantic.ValidationError may be raised.
        If you need to access value without validation, you must
        use `get_context_dict()` instead.
        """
        t = time.time()
        values = self._resolve_context_data(expand_context_names(context))
        # FIXME: reduce string template in all values here!

        if path is not None:
            value = values.dot_get(path)
        else:
            value = values

        dict_value = value.to_dict()

        # FIXME: try/except this line to report that the model_type is missing defaults:
        try:
            defaults = model_type()
        except:
            raise ValueError(
                f"Could not instantiate {model_type} without args. Does it define default for all fields?"
            )

        conformed = self._conform_obj(dict_value, defaults.model_dump())
        model = model_type.model_validate(conformed)

        logger.debug(f"->COMPUTED CONTEXT IN {time.time()-t:.5f}")
        return model

    @classmethod
    def _conform_obj(
        cls, o, schema: list[Any] | dict[str, Any] | Any
    ) -> dict[str, Any] | Any:
        # print("???? O", o)
        # print("???? S", schema)
        if isinstance(schema, list):
            # we don't conform fields with a list value
            # (we let pydantic handle that)
            return o
        if isinstance(o, collections.abc.Mapping):
            if "@keys" in o:
                if not o["@keys"]:
                    # smart early quit, for speed...
                    return []
                items_schema = o["@defaults"]
                return [
                    cls._conform_obj(i, items_schema) for i in o.get("@", {}).values()
                ]
            conformed = dict()
            for k, v in schema.items():
                # print("  -->", k)
                conformed[k] = cls._conform_obj(o.get(k, v), schema[k])
            return conformed
        # TODO: End of Patch
        elif isinstance(o, list):
            conformed = []
            for i in o:
                conformed.append(cls._conform_obj(i, schema))
            return conformed
        return o
