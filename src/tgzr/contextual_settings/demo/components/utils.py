from nicegui import ui


def dict_table(d) -> ui.grid:
    with ui.grid(columns="auto auto") as grid:
        for k, v in d.items():
            with ui.row():
                ui.label(str(k) + ":")
            with ui.row():
                ui.label(str(v))
                ui.space()
    return grid
