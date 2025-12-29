from __future__ import annotations

from typing import Any
import operator
import os
from copy import deepcopy

from .context_data import ContextData


class Op:
    _IS_MODIFIER = True  # Must be set to False if the new value doesn't depend on anything but args
    _ENVIRON_GETTER = lambda: os.environ

    @classmethod
    def get_environ(cls) -> dict[str, str]:
        return cls._ENVIRON_GETTER()  # type: ignore but it works ¯\_(ツ)_/¯

    @classmethod
    def set_environ_getter(cls, getter) -> None:
        """
        This is for testing/demo purpose.
        It allow to use a custom dict instead
        of os.environ.
        """
        cls._ENVIRON_GETTER = getter

    @classmethod
    def reset_environ_getter(cls) -> None:
        cls._ENVIRON_GETTER = lambda: os.environ

    def __init__(self, key: str, *args, **kwargs):
        self.key = key
        self.args = args
        self.kwargs = kwargs

    def summary(self) -> str:
        args = [repr(arg) for arg in self.args]
        kwargs = [f"{k}={v!r}" for k, v in self.kwargs.items()]
        all_args = ", ".join(args + kwargs)
        return f"{self.__class__.__name__} {all_args}"

    def __repr__(self) -> str:
        args = [repr(arg) for arg in self.args]
        kwargs = [f"{k}={v!r}" for k, v in self.kwargs.items()]
        all_args = ", ".join(args + kwargs)
        return f"{self.__class__.__name__}({self.key}, {all_args})"

    def default_base(self):
        """
        The value to use when the base is unset (i.e. == ContextData())."""
        return None

    def is_pinning(self, old_value: Any, new_value: Any) -> bool:
        if self._IS_MODIFIER:
            return False
        return old_value == new_value

    def is_override(self, old_value: Any, new_value: Any) -> bool:
        return old_value != new_value

    def render(
        self,
        on: ContextData,
        history_data: ContextData | None = None,
        context_info: dict[str, str | dict[str, str | bool]] | None = None,
    ) -> None:
        value = on.dot_get(self.key)
        apply_info = {}
        new_value = self.apply(value, apply_info)
        on.dot_set(self.key, new_value)
        if history_data is not None and context_info is not None:
            history = history_data.dot_get(self.key)
            if history == ContextData():
                history = []
            context_info["old_value_repr"] = repr(value)
            context_info["new_value_repr"] = repr(new_value)
            context_info["override_info"] = dict(
                pinned=self.is_pinning(value, new_value),
                overridden=self.is_override(value, new_value),
            )
            context_info["apply_info"] = apply_info
            history.append(context_info)
            history_data.dot_set(self.key, history)

    def render_flat(
        self,
        on: dict[str, Any],
        history_data: dict[str, Any] | None = None,
        context_info: dict[str, str | dict[str, str | bool]] | None = None,
    ) -> None:
        # FIXME: refactor render() and render_flat() to avoid repeating code
        # (I'm tired of bugs introduced after a change to one not done in other)
        value = on.get(self.key, None)
        apply_info = {}
        new_value = self.apply(value, apply_info)
        on[self.key] = new_value
        if history_data is not None and context_info is not None:
            history = history_data.get(self.key, [])
            context_info["old_value_repr"] = repr(value)
            context_info["new_value_repr"] = repr(new_value)
            context_info["override_info"] = dict(
                pinned=self.is_pinning(value, new_value),
                overridden=self.is_override(value, new_value),
            )
            context_info["apply_info"] = apply_info
            history.append(context_info)
            history_data[self.key] = history

    def apply(self, to: Any, apply_info: dict[str, str]) -> Any:
        """
        Subclass must implement this to return
        `to` modified by the Op.

        /!\\ Be carefull to never edit `to` !

        Subclasses can add data to the `apply_info` dict
        to provide information about what happend while
        applying the operator.
        This info is not processed and is meant to be
        presented to the user.
        """
        ...


class Toggle(Op):

    def __init__(self, key: str):
        super().__init__(key)

    def apply(self, to: Any, apply_info: dict[str, str]) -> Any:
        return not bool(to)


class _SingleArgOp(Op):
    """
    Utility base for Op with only a "value" argument.
    """

    def __init__(self, key: str, value: Any):
        super().__init__(key, value)

    @property
    def value(self) -> Any:
        return self.args[0]


class Set(_SingleArgOp):
    _IS_MODIFIER = False

    def apply(self, to: Any, apply_info: dict[str, str]) -> Any:
        if to == self.value:
            apply_info["message"] = "Same value as base."
        return self.value


class Append(_SingleArgOp):

    def apply(self, to: Any, apply_info: dict[str, str]) -> Any:
        if not to or to == ContextData():
            value = []
            apply_info["message"] = "Value initialized to `[]`."
        else:
            value = list(to)
            if value != to:
                apply_info["message"] = "Value coerced to `list`."
        value.append(self.value)
        return value


class EnvOverride(_SingleArgOp):
    """
    Set the value from the given env var only if that
    env var exists.
    """

    def apply(self, to: Any, apply_info: dict[str, str]) -> Any:
        try:
            env_value = self.get_environ()[self.value]
        except KeyError:
            apply_info["message"] = (
                f"Env var ${self.value} not found, keeping value {to!r}"
            )
            return to
        else:
            apply_info["message"] = f"Env var ${self.value} found."
        return env_value


class Remove(_SingleArgOp):

    def apply(self, to: Any, apply_info: dict[str, str]) -> Any:
        if not to:
            value = []
        else:
            try:
                value = list(to)
            except TypeError:
                apply_info["Warning"] = (
                    f"cannot copy base as list: {to}, using empyt list."
                )
                value = []
        try:
            value.remove(self.value)
        except ValueError as err:
            apply_info["Aborted"] = str(err)
            return value
        return value


class _OperatorOp(Op):
    """
    Base class for Op based on `operator` module operation
    These are way faster than python implementation.
    """

    OPERATOR = None

    def apply(self, to: Any, apply_info: dict[str, str]) -> Any:
        if isinstance(to, ContextData):
            to = self.default_base()
        assert (
            self.OPERATOR is not None
        )  # Subclass must override `OPERATOR` class attribute !
        return self.OPERATOR(to, *self.args, **self.kwargs)


class Add(_OperatorOp):
    OPERATOR = operator.add

    def __init__(self, key: str, value: Any):
        super().__init__(key, value)

    def default_base(self):
        if not self.args:
            return None
        return type(self.args[0])()


class Sub(_OperatorOp):
    OPERATOR = operator.sub

    def __init__(self, key: str, value: Any):
        super().__init__(key, value)


class SetItem(_OperatorOp):
    # OPERATOR = operator.setitem

    def __init__(self, key: str, index: int, item_value: Any):
        super().__init__(key, index, item_value)

    def apply(self, to: Any, apply_info: dict[str, str]) -> Any:
        # NB: overridden from _OperatorOp because the operator edits in place
        # and we don't want that.
        if isinstance(to, ContextData):
            to = self.default_base()
        if not isinstance(to, (list, tuple, dict)):
            apply_info["Warning"] = (
                f"cannot set item, base is not a list, tuple or dict: {to}"
            )
            return to
        if isinstance(to, dict):
            copy = deepcopy(to)
        else:
            copy = list(to)
        operator.setitem(copy, *self.args, **self.kwargs)
        return copy


class DelItem(_OperatorOp):
    # OPERATOR = operator.delitem

    def __init__(self, key: str, index: int):
        super().__init__(key, index)

    def apply(self, to: Any, apply_info: dict[str, str]) -> Any:
        # NB: overridden from _OperatorOp because the operator edits in place
        # and we don't want that.
        if isinstance(to, ContextData):
            to = self.default_base()
        if not isinstance(to, (list, tuple, dict)):
            apply_info["Warning"] = (
                f"cannot set item, base is not a list, tuple or dict: {to}"
            )
            return to
        if isinstance(to, dict):
            copy = deepcopy(to)
        else:
            copy = list(to)
        operator.delitem(copy, *self.args, **self.kwargs)
        return copy


class Pop(_OperatorOp):
    OPERATOR = operator.delitem

    def __init__(self, key: str, index: int | slice):
        super().__init__(key, index)


class RemoveSlice(_OperatorOp):
    OPERATOR = operator.delitem

    def __init__(self, key: str, start: int, stop: int, step: int | None = None):
        super().__init__(key, slice(start, stop, step))


class Call(_OperatorOp):
    OPERATOR = operator.methodcaller

    def __init__(self, key, method_name, args, kwargs):
        super().__init__(key, method_name, *args, **kwargs)


class OpBatch:
    def __init__(self):
        self._ops: list[Op] = []

    def append(self, op: Op):
        self._ops.append(op)

    def render(
        self,
        on: ContextData,
        history_data: ContextData | None = None,
        context_info: dict[str, str | dict[str, str | bool]] | None = None,
    ) -> None:
        op_context_info = None
        for op in self._ops:
            if context_info is not None:
                op_context_info = context_info.copy()
                op_context_info["op_name"] = op.__class__.__name__
                op_context_info["op"] = repr(op)
                op_context_info["summary"] = op.summary()
            op.render(on, history_data, op_context_info)

    def render_flat(
        self,
        on: dict[str, Any],
        history_data: dict[str, Any] | None = None,
        context_info: dict[str, str | dict[str, str | bool]] | None = None,
    ) -> None:
        op_context_info = None
        for op in self._ops:
            if context_info is not None:
                op_context_info = context_info.copy()
                op_context_info["op_name"] = op.__class__.__name__
                op_context_info["op"] = repr(op)
                op_context_info["summary"] = op.summary()
            op.render_flat(on, history_data, op_context_info)
