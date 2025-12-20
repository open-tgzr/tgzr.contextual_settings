from typing import Any
from collections import defaultdict

from .base_store import BaseStore, ops
from ..context_data import ContextData


class MemoryStore(BaseStore):
    def __init__(self):
        super().__init__()
        self._context_ops: dict[str, ops.OpBatch] = defaultdict(ops.OpBatch)
        self._context_info: dict[str, dict[str, Any]] = defaultdict(dict)

    def _append_op(self, context_name: str, op: ops.Op) -> None:
        self._context_ops[context_name].append(op)

    def _get_ops(self, context_name) -> ops.OpBatch:
        return self._context_ops[context_name]

    def context_names(self) -> tuple[str, ...]:
        return tuple(self._context_ops.keys())

    def set_context_info(self, context_name: str, **kwargs) -> None:
        self._context_info[context_name].update(kwargs)

    def get_context_info(self, context_name: str) -> dict[str, Any]:
        return self._context_info[context_name]
