from __future__ import annotations

from typing import Any
import operator
import os

from .context_data import ContextData


class Op:

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

    def __init__(self, key: str, value: Any):
        self.key = key
        self.value = value

    def __repr__(self) -> str:
        return f"{__name__}.{self.__class__.__name__}({self.key!r}, {self.value!r})"

    def render(
        self,
        on: ContextData,
        history_data: ContextData | None = None,
        context_info: dict[str, str | dict[str, str]] | None = None,
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
            context_info["new_value"] = new_value
            context_info["override_info"] = dict(
                pinned=value == new_value, overridden=value != new_value
            )
            context_info["apply_info"] = apply_info
            history.append(context_info)
            history_data.dot_set(self.key, history)

    def render_flat(
        self,
        on: dict[str, Any],
        history_data: dict[str, Any] | None = None,
        context_info: dict[str, str | dict[str, str]] | None = None,
    ) -> None:
        # FIXME: refactor render() and render_flat() to avoid repeating code
        # (I'm tired of bud introduced after a change to one not done in other)
        value = on.get(self.key, None)
        apply_info = {}
        new_value = self.apply(value, apply_info)
        on[self.key] = new_value
        if history_data is not None and context_info is not None:
            history = history_data.get(self.key, [])
            context_info["old_value_repr"] = repr(value)
            context_info["new_value_repr"] = repr(new_value)
            context_info["override_info"] = dict(
                pinned=value == new_value, overridden=value != new_value
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


class Set(Op):

    def apply(self, to: Any, apply_info: dict[str, str]) -> Any:
        if to == self.value:
            apply_info["message"] = "Same value as base."
        return self.value


class Append(Op):

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


class EnvOverride(Op):
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


class Remove(Op):

    def apply(self, to: Any, apply_info: dict[str, str]) -> Any:
        if not to:
            value = []
        else:
            value = list(to)
        try:
            value.remove(self.value)
        except ValueError:
            return []
        return value


class _OperatorOp(Op):
    OPERATOR = None

    def __init__(self, key: str, *args, **kwargs):
        super().__init__(key, None)
        self.args = args
        self.kwargs = kwargs

    def __str__(self) -> str:
        args = [repr(arg) for arg in self.args]
        kwargs = [f"{k}={v!r}" for k, v in self.kwargs.items()]
        all_args = ", ".join(args + kwargs)
        return f"{self.__class__.__name__}({self.key}, {all_args})"

    def apply(self, to: Any, apply_info: dict[str, str]) -> Any:
        assert (
            self.OPERATOR is not None
        )  # Subclass must override `OPERATOR` class attribute !
        return self.OPERATOR(to, *self.args, **self.kwargs)


class Add(_OperatorOp):
    OPERATOR = operator.add

    def __init__(self, key: str, value: Any):
        super().__init__(key, value)


class Sub(_OperatorOp):
    OPERATOR = operator.sub

    def __init__(self, key: str, value: Any):
        super().__init__(key, value)


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
        context_info: dict[str, str | dict[str, str]] | None = None,
    ) -> None:
        op_context_info = None
        for op in self._ops:
            if context_info is not None:
                op_context_info = context_info.copy()
                op_context_info["op_name"] = op.__class__.__name__
                op_context_info["op"] = repr(op)
            op.render(on, history_data, op_context_info)

    def render_flat(
        self,
        on: dict[str, Any],
        history_data: dict[str, Any] | None = None,
        context_info: dict[str, str | dict[str, str]] | None = None,
    ) -> None:
        op_context_info = None
        for op in self._ops:
            if context_info is not None:
                op_context_info = context_info.copy()
                op_context_info["op_name"] = op.__class__.__name__
                op_context_info["op"] = repr(op)
            op.render_flat(on, history_data, op_context_info)
