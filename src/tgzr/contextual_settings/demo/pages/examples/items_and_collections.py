from typing import Literal

from nicegui import ui

from ...components import header, left_drawer, conf_explorer

from tgzr.contextual_settings.stores.memory_store import MemoryStore
from tgzr.contextual_settings.items import Item, NamedItem, Collection


class User(NamedItem):
    pass


class Task(NamedItem):
    dept: Literal["anim", "lighting", "comp", "other"] = "other"
    assignee: str | None = None


class Project(Item):
    users: Collection[User] = Collection.Field(User)
    tasks: Collection[Task] = Collection.Field(Task)


_STORE = None


def get_store() -> MemoryStore:
    global _STORE
    if _STORE is None:
        _STORE = _create_store()
    return _STORE


def _create_store() -> MemoryStore:
    store = MemoryStore()
    store.set_context_info("defaults", color="#9996C3", icon="input")
    store.set_context_info("plan_A", color="#FFA600", icon="alt_route")
    store.set_context_info("plan_B", color="#88EDFF", icon="keyboard_option_key")

    project = Project(name="MyProject")
    alice = project.users.add(User, name="alice")
    bob = project.users.add(User, name="bob")

    anim = project.tasks.add(Task, "Anim", dept="anim")

    store.update_context("defaults", project, "project")

    anim.assignee = alice.name
    store.update_context("plan_A", anim, "project.tasks.items.@.Anim")

    anim.assignee = bob.name
    store.update_context("plan_B", anim, "project.tasks.items.@.Anim")

    return store


help_txt = """
## Items and Collections

> ⚠️ Note: This implementation is in progress and the API is subject to changes!

You can use any pydantic model to manipulate context data, but we 
also provide some convenient ones you can inherit from:

- **tgzr.contextual_settings.items.Item**: a model with a name (str) and an uid (UUID)
- **tgzr.contextual_settings.items.NamedItem**: an Item using its name as a unic identifier
- **tgzr.contextual_settings.items.Collection**: an Convenient model to manage a typed list of items

Note: **Item** and **NamedItem** subclasses *MUST* have a default value for every field.

```python 
'''
To build the "Usage" tab on the right panel, we have define 3 models:
    User: has a unic name and an uid.
    Task: has a dept:str field and an optional assignee:User field.
    Project: It has a name and an uid, and collections fields for Users and Tasks.

Here is the code:
'''
from typing import Literal
from tgzr.contextual_settings.items import Item, NamedItem, Collection

class User(NamedItem):
    pass
    
class Task(NamedItem):
    dept: Literal['anim', 'lighting', 'comp', 'other'] = 'other'
    assignee: str|None = None

class Project(Item):
    users : Collection[User] = Collection.Field(User)
    tasks : Collection[Task] = Collection.Field(Task)

'''
Then we create instances of these models:
'''

# Create the project and add some users:
project = Project(name="MyProject")
alice = project.users.add(User, name="alice")
bob = project.users.add(User, name="bob")

# Create the anim task:
anim = project.tasks.add(Task, "Anim", dept="anim")

'''
And now we can use these models to update some context:
'''
store = MemoryStore()

# and store the project at path 'project' in the 'default' context:
store.update_context("defaults", project, "project")

# Assign the taks to alice, store the task in "plan_A" context:
anim.assignee = alice.name
store.update_context("plan_A", anim, "project.tasks.items.@.Anim")

# Assign the taks to bob, store the task in "plan_B" context:
anim.assignee = bob.name
store.update_context("plan_B", anim, "project.tasks.items.@.Anim")


'''
Later, the GUI wants to show the project's anim taks.
It fetches it from the store using the Project model and
a given context:
'''
from something_of_yours import get_store, Project

store = get_store()
project = store.
```

"""


@ui.refreshable
async def anim_task_display(context: list[str] = ["defaults"]):
    ui.label(f"(Using context: {context})")
    try:
        project = get_store().get_context(context, Project, "project")
        anim = project.tasks.get_by(name="Anim")
    except Exception as err:
        ui.label(f"Error getting the Anim task:")
        pre = ui.element("pre")
        pre._text = str(err)
        return

    if anim is None:
        ui.label(f"Could not find the Anim task in context {context}:/")
        return

    assignee = project.users.get_by(name=anim.assignee)
    ui.markdown(
        f"""
### Task: {anim.name}
#### Department: {anim.dept}
#### Assignee: {assignee and assignee.name or 'No Assignee'}
"""
    )


@ui.page("/examples/items_and_collections")
async def app_config_example():
    await header()
    await left_drawer()
    with ui.splitter().classes("w-full") as splitter:
        with splitter.before:
            with ui.scroll_area().classes("h-[90dvh]"):
                ui.markdown(help_txt)
        with splitter.after:
            with ui.tabs().classes("w-full") as tabs:
                ui.tab("Usage")
                ui.tab("Conf")
            with ui.tab_panels(tabs).classes("w-full h-[85dvh]"):
                with ui.tab_panel("Usage").classes("w-full h-full grow") as usage:
                    tabs.value = usage
                    base_context = "defaults"
                    with ui.row():
                        ui.button(
                            base_context,
                            on_click=lambda n=base_context: anim_task_display.refresh(
                                [base_context]
                            ),
                        )
                        for ctx_name in ["plan_A", "plan_B"]:
                            ui.button(
                                ctx_name,
                                on_click=lambda n=ctx_name: anim_task_display.refresh(
                                    [base_context, n]
                                ),
                            )
                    await anim_task_display()
                with ui.tab_panel("Conf"):
                    await conf_explorer(get_store())
