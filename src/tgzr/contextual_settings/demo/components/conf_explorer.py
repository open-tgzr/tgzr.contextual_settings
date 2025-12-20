from __future__ import annotations

from typing import Any, Callable
import time

from nicegui import ui

from tgzr.contextual_settings.stores.base_store import BaseStore

from . import utils

from .sortable.sortable_column import SortableRow


class ContextNameChip(ui.row):
    def __init__(
        self,
        context_name: str,
        info: dict[str, Any],
        /,
        on_click: Callable[[Any], None] | None = None,
    ):
        super().__init__(align_items="center")
        ui.chip
        self.context_name = context_name
        self.icon = info.get("icon") or "list"  # "rule"
        self.color = info.get("color", "#dddddd")
        self.description = info.get("description")
        self.border_color = "#88FF88"
        self._on_click_cb = on_click

        self._mute = False

        self._style["background-color"] = self.color
        self._style["border-color"] = self.color
        self._style["border-width"] = "1px"
        self._style["border-radius"] = "50vh"

        self.classes("p-2 m-1 gap-1")

        if on_click is not None:
            self.on("click", on_click)

        with self:
            with ui.icon(self.icon).props("size=1rem"):
                with ui.tooltip():
                    expanded_names = info.get("expanded_context_names")
                    if expanded_names:
                        ui.label("Context name expanded to:")
                        for cn in expanded_names:
                            ui.label(cn).classes("text-bold")
                        ui.label("-----")
                    ui.label("LMB = Toggle this context.")
                    ui.label(
                        "Ctrl+LMB = Use only this context / Use all contexts up to this one."
                    )
                    ui.label("Alt+LMB = Use all contexts / all contexts but this one.")
                    ui.label("Ctrl+Alt+LMB = Use all contexts up to this one.")

            self.label = ui.label(self.context_name).classes("cursor-pointer")
            if self.description:
                with self.label:
                    ui.tooltip(self.description)

    def update_style(self):
        self.label.set_visibility(True)
        self._style["background-color"] = not self.mute and self.color or None
        self.update()

    @property
    def mute(self):
        return self._mute

    def set_mute(self, b: bool) -> None:
        self._mute = b
        self.update_style()


class ContextNameChips(ui.row):
    def __init__(
        self,
        store: BaseStore,
        on_context_names_changed=Callable[[list[str]], None],
        hide_add_input: bool = False,
    ):
        super().__init__(align_items="center")
        self.classes("w-full")
        self.store = store
        self._on_context_names_changed = on_context_names_changed

        with self:
            self.chip_container = SortableRow(on_change=self._on_order_change).classes(
                "gap-0"
            )

            self.add_input = ui.input("Add Context").on(
                "keydown.enter", lambda: self.add_context_name(self.add_input.value)
            )
            with self.add_input.add_slot("append"):
                ui.button(
                    icon="add",
                    on_click=lambda: self.add_context_name(self.add_input.value),
                ).props("round dense flat")

        if hide_add_input:
            self.add_input.visible = False

    def add_context_name(self, context_name: str) -> None:
        if not context_name:
            # Do not accept empty context name
            return
        if context_name in [c.context_name for c in self.get_chips()]:
            # Do not duplicate context name
            return
        info = self.store.get_context_info(context_name)
        expanded_context_names = self.store.expand_context_name(context_name)
        if expanded_context_names != [context_name]:
            info["expanded_context_names"] = expanded_context_names
        with self.chip_container:
            ContextNameChip(
                context_name,
                info,
                on_click=self._on_chip_click,
            )

    def get_chips(self) -> list[ContextNameChip]:
        return [i for i in self.chip_container]  # type: ignore

    def get_active_chips(self) -> list[ContextNameChip]:
        chips = [chip for chip in self.get_chips() if not chip.mute]
        return chips

    def _on_chip_click(self, event):
        event_chip: ContextNameChip = event.sender
        chips = self.get_chips()
        # if the chip is from another parent, we still want
        # to affect our chips, so we resolve by name instead
        # of just using the event.sender:
        candidat_chips = [c for c in chips if c.context_name == event_chip.context_name]
        if not candidat_chips:
            # maybe our chip has this context in its expanded context names:
            candidat_chips = [
                c
                for c in chips
                if event_chip.context_name
                in self.store.expand_context_name(c.context_name)
            ]
            if not candidat_chips:
                return
        chip = candidat_chips[0]
        if event.args["ctrlKey"] and event.args["altKey"]:
            mute = False
            for c in chips:
                c.set_mute(mute)
                if c == chip:
                    mute = True
        elif event.args["ctrlKey"]:
            if [c for c in chips if not c.mute] == [chip]:
                mute = False
                for c in chips:
                    c.set_mute(mute)
                    if c == chip:
                        mute = True
            else:
                [c.set_mute(True) for c in chips]
                chip.set_mute(False)
        elif event.args["altKey"]:
            if chip.mute:
                chip.set_mute(False)
            else:
                [c.set_mute(False) for c in chips]
                chip.set_mute(True)
        else:
            chip.set_mute(not chip.mute)

        context_names = [chip.context_name for chip in self.get_active_chips()]
        self._on_context_names_changed(context_names)

    def _on_order_change(
        self, new_index: int, old_index: int, new_list: int, old_list: int
    ):
        assert new_list == old_list
        # bake the order in the children list, because that's what we
        # use to get the context names:
        chip = self.chip_container.default_slot.children.pop(old_index)
        self.chip_container.default_slot.children.insert(new_index, chip)
        context_names = [chip.context_name for chip in self.get_active_chips()]
        # update the view:
        self._on_context_names_changed(context_names)


class ConfExplorer(ui.column):
    # TODO: consider adding a tree view mode
    # maybe use this? https://gist.github.com/flooxo/87e244a5717087c2b22af110874380c1

    def __init__(self, store: BaseStore, context_names: list[str] | None):
        super().__init__()
        self.store = store
        self.rows = []
        self.details_row_index = None
        self.current_row_key = None
        self.context_names = context_names or list(store.context_names())

        with self:
            self.context_name_chips = ContextNameChips(
                self.store, self._on_context_names_changed
            )
            for context_name in self.context_names:
                self.context_name_chips.add_context_name(context_name)

            with ui.splitter().classes("w-full gap-2") as splitter:
                with splitter.before:
                    self.value_table()  # type: ignore ui.refreshable miss-annotated
                with splitter.after:
                    self.details_panel()  # type: ignore ui.refreshable miss-annotate

        self.classes("w-full")

    @ui.refreshable
    def details_panel(self):
        with ui.column().classes("w-full"):
            if self.details_row_index is None:
                ui.label("Select a value...")
            else:
                try:
                    row = self.rows[self.details_row_index]
                except:
                    ui.label(
                        f"Select another value... (error getting row {self.details_row_index})"
                    )
                    return
                with ui.column():
                    k = row["key"]
                    v = row["value"]
                    ui.label(f"{k} = {v!r}").classes("text-h5")
                    ui.label(f"{len(row['history'])} operation(s) involved:").classes(
                        "text-h6"
                    )
                    with ui.grid(columns="auto auto auto auto").classes(
                        "place-content-center"
                    ):
                        for e in row["history"]:
                            with ui.row(align_items="start"):
                                context_name = e["context_name"]
                                context_info = self.store.get_context_info(context_name)
                                ContextNameChip(
                                    context_name,
                                    context_info,
                                    on_click=self.context_name_chips._on_chip_click,
                                )
                            with ui.row(align_items="center"):
                                with ui.label(e["op_name"]):
                                    ui.tooltip(e["op"])
                            with ui.row(align_items="center"):
                                ui.label(
                                    f'{e["old_value_repr"]} -> {e["new_value_repr"]}'
                                )
                            with ui.row(align_items="center"):
                                apply_info = e["apply_info"]
                                if apply_info:
                                    utils.dict_table(apply_info)

    @ui.refreshable
    def value_table(self):
        t = time.time()
        flat = self.store.get_context_flat(self.context_names, with_history=True)
        elapsed = time.time() - t

        history = flat.pop("__history__", {})
        self.rows = []
        for row_index, (k, v) in enumerate(flat.items()):
            first_context = "???"
            last_context = ""
            h = history.get(k)
            if h:
                first_context = h[0]["context_name"]
                last_context = h[-1]["context_name"]
            row = dict(
                key=k,
                value=v or repr(v),
                history=history.get(k, []),
                ops_count=len(history.get(k, [])),
                first_context=first_context,
                last_context=last_context,
            )
            self.rows.append(row)
            if k == self.current_row_key:
                self.details_row_index = row_index

        ui.label(f"Config computed in {elapsed:.6f} seconds")
        grid = (
            ui.aggrid(
                {
                    "columnDefs": [
                        {
                            "headerName": "Key",
                            "field": "key",
                            # "checkboxSelection": True,
                        },
                        {"headerName": "Value", "field": "value"},
                        {"headerName": "Nb Ops", "field": "ops_count"},
                        {"headerName": "First", "field": "first_context"},
                        {"headerName": "Last", "field": "last_context"},
                    ],
                    "rowData": self.rows,
                    "rowSelection": "single",
                },
                # auto_size_columns=False,
            )
            .on("cellClicked", self._on_value_select)
            .classes("h-[37rem]")
        )

        if self.details_row_index is not None:
            grid.run_row_method(str(self.details_row_index), "setSelected", True)

    def _on_value_select(self, event):
        self.current_row_key = event.args["data"]["key"]
        self.details_row_index = event.args["rowIndex"]
        self.details_panel.refresh()

    def _on_context_names_changed(self, context_names: list[str]) -> None:
        self.context_names = context_names
        self.value_table.refresh()
        self.details_panel.refresh()

    def refresh_all(self):
        self.value_table.refresh()
        self.details_panel.refresh()


async def conf_explorer(store: BaseStore, context_names: list[str] | None = None):
    return ConfExplorer(store, context_names)
