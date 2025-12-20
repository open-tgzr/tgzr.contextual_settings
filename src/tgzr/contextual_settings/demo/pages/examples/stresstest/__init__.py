import time
from collections import defaultdict

from nicegui import ui, background_tasks, run

from ....components import header, left_drawer, conf_explorer

from tgzr.contextual_settings.stores.memory_store import MemoryStore
from ....components.conf_explorer import ContextNameChips, ConfExplorer

_STORE = None
_CE: ConfExplorer | None = None


def get_store() -> MemoryStore:
    global _STORE
    if _STORE is None:
        _STORE = _create_store()
    return _STORE


def _create_store() -> MemoryStore:
    store = MemoryStore()

    # DEFAULT CONTEXT
    store.set_context_info(
        "default",
        color="#FFFA77",
        icon="settings_suggest",
        description="Sets search visible and adds Bookmark and News tabs with their links",
    )

    return store


@ui.refreshable
def show_stats():
    store = get_store()

    avertage_on = 5
    flat_config = None
    time_stats = defaultdict(list)
    content_stats = {"Nb Keys": 0, "Nb Ctxs": "", "Nb Ops": 0}

    def reset_stats():
        flat_config = None
        time_stats.clear()
        content_stats.update({"Nb Keys": 0, "Nb Ctxs": "", "Nb Ops": 0})

    def update_time_stats():
        print("uptade time stats")
        ce = _CE
        if store is not None and ce is not None:
            for i in range(avertage_on):
                t = time.time()
                flat = store.get_context_flat(ce.context_names, with_history=False)
                time_stats["Flat"].append(time.time() - t)

                t = time.time()
                flat = store.get_context_flat(ce.context_names, with_history=True)
                time_stats["Flat w/ History"].append(time.time() - t)

                t = time.time()
                deep = store.get_context_dict(ce.context_names, with_history=False)
                time_stats["Deep"].append(time.time() - t)

                t = time.time()
                deep = store.get_context_dict(ce.context_names, with_history=True)
                time_stats["Deep w/ History"].append(time.time() - t)

                update_echart()

    def update_content_stats():
        ce = _CE
        if flat_config is not None:
            content_stats["Nb Keys"] = len(flat_config)

        if ce is not None:
            content_stats["Nb Ctxs"] = f"{len(ce.context_names)} ({ce.context_names})"

        if store is not None:
            nb_ops = 0
            for context, ops_batch in store._context_ops.items():
                nb_ops += len(ops_batch._ops)
            content_stats["Nb Ops"] = nb_ops

    async def update_stats():
        reset_stats()
        await run.io_bound(update_time_stats)
        update_content_stats()
        update_echart()

    def update_echart():
        print(" refresh")
        series = []
        for k, vl in time_stats.items():
            series.append(dict(type="bar", name=k, data=vl, realtimeSort=False))
        echart.options["series"] = series
        echart.update()

    with ui.column().classes("w-full"):
        ui.button("Update Stats", on_click=update_stats)
        with ui.row().classes("w-full"):
            echart = ui.echart(
                {
                    "xAxis": {"type": "category"},
                    "yAxis": {"type": "value"},
                    "legend": {"textStyle": {"color": "gray"}},
                    "series": [],
                }
            ).classes("min-w-800")
        with ui.row().classes("w-full"):
            ui.label("Content:")
            with ui.grid(columns="auto auto").classes("pl-5"):
                for k, v in content_stats.items():
                    ui.label(k)
                    ui.label(v)


@ui.page("/examples/stresstest")
async def app_config_example():
    global _CE
    await header()
    await left_drawer()

    with ui.grid(columns="auto 1fr").classes("w-full"):
        with ui.column():
            lb_key_depths = ui.input("key depth", value="1")
            btn_plus_10_keys = ui.button("Set 10 keys")
            btn_plus_100_keys = ui.button("Set 100 keys")
            btn_plus_1000_keys = ui.button("Set 1000 keys")
            ui.separator()
            btn_reset = ui.button("Reset Store", icon="restart_alt")
        # with ui.column():
        #     btn_plus_100_ctxs = ui.button("Add 10 Context")
        #     btn_plus_1000_ctxs = ui.button("Add 100 Context")
        # ui.separator().props("vertical")
        with ui.column().classes("w-full"):
            # ui.button("Update Stats", icon="query_stats", on_click=show_stats.refresh)
            show_stats()

    ui.separator()

    store = get_store()
    ce = await conf_explorer(store)
    _CE = ce

    def set_keys(ce: ConfExplorer, nb, key_depth):
        chips = ce.context_name_chips.get_active_chips()
        if not chips:
            ui.notify("No ctx to add keys to. Please enable at least one")
            return

        context_name = chips[-1].context_name
        for i in range(nb):
            path = [f"KEY_{i:05}"]
            for d in range(key_depth):
                store.set(context_name, ".".join(path) + ".value", time.time())
                path.append(f"child_{d:03}")

        ce.refresh_all()

    btn_plus_10_keys.on_click(
        lambda e, ce=ce, nb=10, d=lb_key_depths: set_keys(ce, nb, int(d.value))
    )
    btn_plus_100_keys.on_click(
        lambda e, ce=ce, nb=100, d=lb_key_depths: set_keys(ce, nb, int(d.value))
    )
    btn_plus_1000_keys.on_click(
        lambda e, ce=ce, nb=1000, d=lb_key_depths: set_keys(ce, nb, int(d.value))
    )

    # def add_ctxs(ce: ConfExplorer, nb):
    #     print(ce)
    #     print(nb)

    # btn_plus_100_ctxs.on_click(lambda e, ce=ce, nb=10: add_ctxs(ce, nb))
    # btn_plus_1000_ctxs.on_click(lambda e, ce=ce, nb=100: add_ctxs(ce, nb))

    def reset_config():
        store._context_ops.clear()  # :[
        ce.refresh_all()

    btn_reset.on_click(reset_config)
