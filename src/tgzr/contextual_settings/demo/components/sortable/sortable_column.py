from typing import Callable, Optional
from nicegui import ui


class _Sortable(ui.element, component="sortable_column.js"):
    _nicegui_class = None

    sortable_list = {}

    def __init__(
        self, *, on_change: Optional[Callable] = None, group: str | None = None
    ) -> None:
        super().__init__()
        self.on("item-drop", self.drop)
        self.on_change = on_change

        self._classes.append(self._nicegui_class)
        self._props["group"] = group
        SortableColumn.sortable_list[self.id] = self

    def drop(self, e) -> None:
        if self.on_change:
            self.on_change(
                e.args["new_index"],
                e.args["old_index"],
                SortableColumn.sortable_list[e.args["new_list"]],
                SortableColumn.sortable_list[e.args["old_list"]],
            )
        else:
            print(e.args)

    def makeSortable(self) -> None:
        self.run_method("makesortable")


class SortableColumn(_Sortable):
    _nicegui_class = "nicegui-column"


class SortableRow(_Sortable):
    _nicegui_class = "nicegui-row"
