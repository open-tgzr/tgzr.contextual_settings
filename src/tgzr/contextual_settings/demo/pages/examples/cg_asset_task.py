from typing import Any, Annotated
import uuid

from nicegui import ui, app
import pydantic

from tgzr.contextual_settings.stores.memory_store import MemoryStore
from tgzr.contextual_settings.stores.base_store import BaseStore
from tgzr.contextual_settings.context_name import expand_context_name

from ...components import header, left_drawer, conf_explorer
from ...components.pydantic_form import form_from_pydantic

_STORE = None


#
# ----- STUDIO MODELS
#


class PluginSettings(pydantic.BaseModel):
    require: str = ""


class PluginRequires(pydantic.BaseModel):
    plugin_names: Annotated[
        list[str],
        dict(names_for="plugin_requires", getter="get_require"),
    ] = []

    plugin_requires: list[tuple[str, str]] = []

    def get_require(self, plugin_name: str) -> str | None:
        return dict(self.plugin_requires).get(plugin_name)


class StudioConf(PluginRequires):
    studio_name: str = "You Studio Name"
    icon: str | None = None
    is_studio: bool = False
    is_prod: bool = False
    is_staging: bool = False

    project_codes: list[str] = []


#
# ----- PLUGIN SETTINGS MODEL
#
class BasePluginSettings(pydantic.BaseModel):
    pass


# --- Entities


class EntityTaskConfig(pydantic.BaseModel):
    icon: str | None = None
    title: str | None = None


class EntitiesPluginSettings(BasePluginSettings):
    task_types: Annotated[list[str], dict(keys_for="tasks", getter="get_task")] = []
    tasks: dict[str, EntityTaskConfig] = {}

    def get_task(self, taks_type: str) -> EntityTaskConfig:
        return self.tasks[taks_type]


class EntitiesPluginSettings_v0_1_2(EntitiesPluginSettings):
    pass


# --- Launcher


class LauncherPluginSettings(BasePluginSettings):
    package_index_url: str = "unset"


class LauncherPluginSettings_v0_1_2(LauncherPluginSettings):
    allow_uv: bool = False
    use_simple_index: bool = True


class LauncherPluginSettings_v0_1_3(LauncherPluginSettings_v0_1_2):
    allow_uv: bool = True


class LauncherPluginSettings_v1_2_3(LauncherPluginSettings_v0_1_3):
    auto_update: bool = True


# --- Application Plugins
class MultiPlatformPath(pydantic.BaseModel):
    macos: str | None = None
    linux: str | None = None
    windows: str | None = None


class ApplicationPluginSettings(BasePluginSettings):
    version: str | None = None
    path: MultiPlatformPath = MultiPlatformPath()


# --- Blender


class BlenderPluginSettings(ApplicationPluginSettings):
    version: str | None = "4.4"
    path: MultiPlatformPath = MultiPlatformPath(
        linux="/path/to/blenders",
        macos="/path/to/blenders",
        windows="C:\\Path\\To\\Blenders",
    )


class BlenderPluginSettings_v1_2_3(BlenderPluginSettings):
    pass


# --- Krita


class KritaPluginSettings(ApplicationPluginSettings):
    pass


class KritaPluginSettings_v1_2_3(KritaPluginSettings):
    pass


#
# ----- PROJECT
#


class ImagingSettings(pydantic.BaseModel):
    width: int | None = None
    height: int | None = None


class ProjectSettings(pydantic.BaseModel):
    imaging: ImagingSettings = ImagingSettings()
    framerate: int | None = None


class ProjectConfig(pydantic.BaseModel):
    title: str | None = None
    code: str | None = None
    icon: str | None = None
    is_studio: bool = False
    is_prod: bool = True

    settings: ProjectSettings = ProjectSettings()


class ProjectContext(pydantic.BaseModel):
    project: ProjectConfig = ProjectConfig()


#
# ----- ENTITIES
#
class EntityContext(PluginRequires):
    eid: uuid.UUID | None = None
    etype: str | None = None
    is_shot: bool | None = None
    is_asset: bool | None = None
    is_task: bool | None = None
    task_type: str | None = None
    is_staging: bool | None = None
    category: str | None = None
    project: ProjectConfig = ProjectConfig()
    casting: list[str] = []

    children_entities: list[str] = []


#
# ----- STORE INIT
#
def get_store() -> MemoryStore:
    global _STORE
    if _STORE is None:
        _STORE = _create_store()
    return _STORE


def _init_studio_context(store: BaseStore):
    store.set_context_info(
        "Studio",
        color="#9996C3",
        icon="business",
        description="Default config for the whole Studio",
    )

    # Enjoying autocompletion and type checking like never here ✨🤗✨

    studio_config = StudioConf()
    studio_config.studio_name = "MyStudio"
    studio_config.icon = "studio_icon.svg"
    studio_config.is_studio = True
    studio_config.is_prod = False
    studio_config.is_staging = False

    studio_config.project_codes = ["myprj", "other-prj"]

    studio_config.plugin_names = ["entities", "launcher"]
    studio_config.plugin_requires = [("entities", "==0.1.2"), ("launcher", "==0.1.3")]
    store.update_context("Studio", studio_config, exclude_defaults=False)

    entity_settings_v0_1_2 = EntitiesPluginSettings_v0_1_2()
    entity_settings_v0_1_2.task_types.append("paint")
    entity_settings_v0_1_2.tasks["paint"] = EntityTaskConfig(
        icon="brush", title="Painting"
    )
    entity_settings_v0_1_2.task_types.append("anim")
    entity_settings_v0_1_2.tasks["anim"] = EntityTaskConfig(
        icon="direction_run", title="Animation"
    )
    store.update_context(
        "Studio",
        entity_settings_v0_1_2,
        "plugin_settings.entities.0.1.2",
        exclude_defaults=False,
    )

    launcher_settings_v0_1_2 = LauncherPluginSettings_v0_1_2()
    store.update_context(
        "Studio",
        launcher_settings_v0_1_2,
        "plugin_settings.launcher.0.1.2",
        exclude_defaults=False,
    )

    launcher_settings_v0_1_3 = LauncherPluginSettings_v0_1_3()
    store.update_context(
        "Studio",
        launcher_settings_v0_1_3,
        "plugin_settings.launcher.0.1.3",
        exclude_defaults=False,
    )

    launcher_settings_v1_2_3 = LauncherPluginSettings_v1_2_3()
    store.update_context(
        "Studio",
        launcher_settings_v1_2_3,
        "plugin_settings.launcher.1.2.3",
        exclude_defaults=False,
    )


def _init_plugins_context(store: BaseStore):
    store.set_context_info(
        "Plugins",
        color="#9996C3",
        icon="extension",
        description="Studio Plugins Requires and Settings",
    )

    store.append("Plugins", "plugin_names", "blender")
    store.append("Plugins", "plugin_requires", ("blender", "==1.2.3"))
    store.update_context(
        "Plugins",
        BlenderPluginSettings_v1_2_3(),
        "plugin_settings.blender.1.2.3",
        exclude_defaults=False,
    )
    unsupported_blender_version_settings = dict(
        this_is_an_old_version_of_the_plugin=True,
        you_dont_have_a_model_for_it=True,
        you_are_screwed=False,
        thanks_to_what="That awesome library !!! ✨🥰✨",
    )
    store.update_context_flat(
        "Plugins",
        unsupported_blender_version_settings,
        "plugin_settings.blender.1.2.2",
    )

    store.append("Plugins", "plugin_names", "krita")
    store.update_context(
        "Plugins",
        KritaPluginSettings_v1_2_3(),
        "plugin_settings.krita.1.2.3",
        exclude_defaults=False,
    )


def _init_my_project_context(store: BaseStore):
    store.set_context_info(
        "Project:mypr",
        color="#FFA600",
        icon="theaters",
        description="Project config",
    )

    context = ProjectContext()
    context.project.code = "myprj"
    context.project.title = "My Project"
    context.project.is_prod = True
    context.project.settings.imaging.width = 720
    context.project.settings.imaging.height = 576
    context.project.settings.framerate = 25
    store.update_context("Project:myprj", context)


def _init_other_project_context(store: BaseStore):
    store.set_context_info(
        "Project:myprj",
        color="#FFA600",
        icon="theaters",
        description="Config for 'My Project'",
    )
    store.set_context_info(
        "Project:other-prj",
        color="#FFA600",
        icon="theaters",
        description="Config for 'Other Project'",
    )

    context = ProjectContext()
    context.project.code = "other-prj"
    context.project.title = "Other Project"
    context.project.is_prod = True
    context.project.settings.imaging.width = 800
    context.project.settings.imaging.height = 600
    context.project.settings.framerate = 12
    store.update_context("Project:other-prj", context)


def _init_entities_context(store: BaseStore):

    for name in expand_context_name("[Entity:myprj/assets/props/Table/tex]", {}):
        store.set_context_info(
            name,
            color="#FFFA77",
            icon="account_tree",
            description="Entity hierarchy config",
        )

    mprj = EntityContext()
    mprj.eid = uuid.uuid4()
    mprj.etype = "ProjectRoot"
    mprj.project.settings.imaging.width = 1920
    mprj.project.settings.imaging.height = 1080
    mprj.project.settings.framerate = 24
    mprj.is_staging = False
    mprj.children_entities = ["assets", "shots"]
    store.update_context("Entity:myprj", mprj)

    assets = EntityContext(
        eid=uuid.uuid4(),
        etype="AssetLibrary",
        is_shot=False,
        is_asset=True,
    )
    assets.children_entities = ["props", "chars", "sets"]
    store.update_context("Entity:myprj/assets", assets)

    props = EntityContext(
        eid=uuid.uuid4(),
        etype="AssetCategory",
        category="PROPS",
    )
    props.children_entities = ["Table", "Glass", "Plate", "Fork", "Knife"]
    store.update_context("Entity:myprj/assets/props", props)

    table_prop = EntityContext(
        eid=uuid.uuid4(),
        etype="Asset",
    )
    table_prop.children_entities = ["mod", "rig", "tex", "package"]
    store.update_context("Entity:myprj/assets/props/Table", table_prop)

    table_custom_blender_settings = BlenderPluginSettings_v1_2_3(version="4.5")
    store.update_context(
        "Entity:myprj/assets/props/Table",
        table_custom_blender_settings,
        path="plugin_settings.blender.1.2.3",
    )

    table_tex = EntityContext(
        eid=uuid.uuid4(),
        etype="AssetTask",
        is_task=True,
        task_type="paint",
        casting=["Glass", "Plate"],
    )
    table_tex.children_entities = ["scene", "products"]
    store.update_context("Entity:myprj/assets/props/Table/tex", table_tex)

    store.append("Entity:myprj/assets/props/Table/tex", "casting", "Fork")
    store.append("Entity:myprj/assets/props/Table/tex", "casting", "Knife")

    store.remove("Entity:myprj/assets/props/Table/tex", "plugin_names", "blender")


def _init_staging_context(store: BaseStore):
    store.set_context_info(
        "Dev:Alice",
        color="#FF7B00",
        icon="bug_report",
        description="Dev override for Alice's branch",
    )

    alice_dev_entity_overrides = EntityContext(is_staging=True)
    store.update_context("Dev:Alice", alice_dev_entity_overrides)

    alice_dev_custom_blender_settings = BlenderPluginSettings_v1_2_3(version="4.6b")
    store.update_context(
        "Dev:Alice",
        alice_dev_custom_blender_settings,
        path="plugin_settings.blender.1.2.3",
    )


def _create_store() -> MemoryStore:
    env = app.custom_env  # type: ignore wallah c'est moi qui l'a mis j'te jure ma race.
    env["PROJECT"] = "myprj"
    env["STAGING"] = "Dev:Alice"

    store = MemoryStore()

    # Settings info for resolvable contexts:
    store.set_context_info(
        "Project:$PROJECT",
        color="#90FF7F",
        icon="theaters",
        description="Project config resolved from envvar",
    )
    store.set_context_info(
        "[Entity:myprj/assets/props/Table/tex]",
        color="#FFFA77",
        icon="account_tree",
        description='The "current" entity, and all its ancestors',
    )
    store.set_context_info(
        "$STAGING",
        color="#FF7B00",
        icon="bug_report",
        description="Overrides for Dev, configured by the $STATING envvar.",
    )

    _init_studio_context(store)
    _init_plugins_context(store)
    _init_my_project_context(store)
    _init_other_project_context(store)
    _init_entities_context(store)
    _init_staging_context(store)
    return store


# def x_create_store() -> MemoryStore:
#     env = app.custom_env  # type: ignore wallah c'est moi qui l'a mis j'te jure ma race.
#     env["STAGING"] = "Dev:Alice"

#     store = MemoryStore()

#     # --- STUDIO

#     # Studio info
#     store.set("Studio", "studio_name", "MyStudio")
#     store.set("Studio", "icon", "studio_icon.svg")
#     store.set("Studio", "is_studio", True)
#     store.set("Studio", "is_prod", False)
#     store.set("Studio", "is_staging", False)

#     # Studio plugins
#     store.append("Plugins", "studio_plugins.names", "entities")
#     store.append("Plugins", "studio_plugins.names", "launcher")
#     store.append("Plugins", "studio_plugins.names", "blender")
#     store.append("Plugins", "studio_plugins.names", "krita")

#     # Plugin requires & settings

#     # entities
#     store.set("Plugins", "plugins.requires.entities", "==0.1.2")
#     store.append("Plugins", "plugins.settings.entities.0.1.2.task_types", "paint")
#     store.set(
#         "Plugins", "plugins.settings.entities.0.1.2.tasks.paint.title", "Painting"
#     )
#     store.set("Plugins", "plugins.settings.entities.0.1.2.tasks.paint.icon", "brush")
#     store.append("Plugins", "plugins.settings.entities.0.1.2.task_types", "anim")
#     store.set(
#         "Plugins", "plugins.settings.entities.0.1.2.tasks.anim.title", "Animation"
#     )
#     store.set(
#         "Plugins", "plugins.settings.entities.0.1.2.tasks.anim.icon", "direction_run"
#     )

#     # launcher
#     store.set("Plugins", "plugins.requires.launcher", "==0.1.3")
#     store.set(
#         "Plugins",
#         "plugins.settings.launcher.1.2.3.package_index_url",
#         "https://pypi.org/simple",
#     )
#     store.set("Plugins", "plugins.settings.launcher.1.2.3.allow_uv", True)
#     store.set(
#         "Plugins", "plugins.settings.launcher.0.1.2.package_index_url", "test.pypi.org"
#     )
#     store.set(
#         "Plugins", "plugins.settings.launcher.0.1.3.package_index_url", "www.pypi.org"
#     )
#     store.set("Plugins", "plugins.settings.launcher.0.1.3.use_simple_index", False)
#     store.set("Plugins", "plugins.settings.launcher.0.1.2.use_simple_index", False)

#     # blender
#     store.set("Plugins", "plugins.requires.blender", "==2.3.4")
#     store.set("Plugins", "plugins.settings.blender.2.3.4.version", "4.4")
#     store.set("Plugins", "plugins.settings.blender.2.3.4.path", "/path/to/blender")

#     # krita
#     store.set("Plugins", "plugins.requires.krita", "==3.4.5")
#     store.set("Plugins", "plugins.settings.krita.3.4.5.version", "5.2")
#     store.set("Plugins", "plugins.settings.krita.3.4.5.path", "/path/to/krita")

#     # --- PROJECT

#     # project info
#     store.set("Project:MyProject", "project.code", "MPRJ")
#     store.set("Project:MyProject", "icon", "mprj_icon.svg")
#     store.set("Project:MyProject", "is_studio", False)
#     store.set("Project:MyProject", "is_prod", True)

#     # entities attributes
#     store.set("Entity:MPRJ", "entity.uuid", uuid.uuid4())
#     store.set("Entity:MPRJ", "project.settings.imaging.width", 1920)
#     store.set("Entity:MPRJ", "project.settings.imaging.heigth", 1080)
#     store.set("Entity:MPRJ", "project.settings.framerate", 24)
#     store.set("Entity:MPRJ", "is_staging", False)

#     store.set("Entity:MPRJ/assets", "entity.uuid", uuid.uuid4())
#     store.set("Entity:MPRJ/assets", "entity.is_shot", False)
#     store.set("Entity:MPRJ/assets", "entity.is_asset", True)
#     store.set("Entity:MPRJ/assets", "project.settings.imaging.width", 1920 / 2)
#     store.set("Entity:MPRJ/assets", "project.settings.imaging.heigth", 1080 / 2)

#     store.set("Entity:MPRJ/assets/props", "entity.uuid", uuid.uuid4())
#     store.set("Entity:MPRJ/assets/props", "entity.asset_type", "PROPS")

#     store.set("Entity:MPRJ/assets/props/Table", "entity.uuid", uuid.uuid4())
#     store.set(
#         "Entity:MPRJ/assets/props/Table",
#         "plugins.settings.blender.2.3.4.version",
#         "4.3",
#     )

#     store.set("Entity:MPRJ/assets/props/Table/Tex", "entity.is_task", True)
#     store.set("Entity:MPRJ/assets/props/Table/Tex", "entity.task_type", "paint")
#     store.append("Entity:MPRJ/assets/props/Table/Tex", "entity.casting", "Glass")
#     store.append("Entity:MPRJ/assets/props/Table/Tex", "entity.casting", "Plate")
#     store.append("Entity:MPRJ/assets/props/Table/Tex", "entity.casting", "Fork")
#     store.append("Entity:MPRJ/assets/props/Table/Tex", "entity.casting", "Knife")

#     # --- STAGING

#     # Alice's dev settings
#     store.set("Dev:Alice", "is_staging", True)
#     store.set("Dev:Alice", "plugins.settings.blender.2.3.4.version", "5.0")

#     store.set_context_info(
#         "Studio",
#         color="#9996C3",
#         icon="business",
#         description="Default config for the whole Studio",
#     )
#     store.set_context_info(
#         "Plugins",
#         color="#9996C3",
#         icon="business",
#         description="Studio Plugins Requires and Settings",
#     )
#     store.set_context_info(
#         "Project:MyProject",
#         color="#FFA600",
#         icon="theaters",
#         description="Project config",
#     )
#     store.set_context_info(
#         "[Entity:MPRJ/assets/props/Table/Tex]",
#         color="#FFFA77",
#         icon="account_tree",
#         description='The "current" entity, and all its ancestors',
#     )
#     for name in expand_context_name("[Entity:MPRJ/assets/props/Table/Tex]", {}):
#         store.set_context_info(
#             name,
#             color="#FFFA77",
#             icon="account_tree",
#             description="Entity hierarchy config",
#         )
#     store.set_context_info(
#         "Dev:Alice",
#         color="#FF7B00",
#         icon="bug_report",
#         description="Dev override for Alice's branch",
#     )
#     store.set_context_info(
#         "$STAGING",
#         color="#FF7B00",
#         icon="bug_report",
#         description="Overrides for Dev, configured by the $STATING envvar.",
#     )

#     return store


#
# --------- Example Frontend
#


# class BaseConf(pydantic.BaseModel):
#     is_studio: bool = False
#     is_prod: bool = False
#     is_staging: bool = False


# class PluginVersionPatch(pydantic.BaseModel):
#     settings: Any = None


# class PluginVersionMinor(pydantic.BaseModel):
#     version_patch: PluginVersionPatch = pydantic.Field(
#         default_factory=PluginVersionPatch
#     )


# class PluginVersionMajor(pydantic.BaseModel):
#     version_minor: PluginVersionMinor = pydantic.Field(
#         default_factory=PluginVersionMinor
#     )


# class PluginSettings(pydantic.BaseModel):
#     name: str = "PluginName"
#     version_major: PluginVersionMajor = pydantic.Field(
#         default_factory=PluginVersionMajor
#     )


# class PluginSettings(pydantic.BaseModel):
#     require: str = ""


# class PluginsConfig(pydantic.BaseModel):
#     names: list[str] = []
#     # requires: list[str] = []
#     # settings: list[PluginSettings] = []


# class StudioConf(BaseConf):
#     studio_name: str = "You Studio Name"
#     icon: str | None = None
#     studio_plugins: PluginsConfig = pydantic.Field(default_factory=PluginsConfig)


#
# Plugin Settings
#
# class BasePluginSettings(pydantic.BaseModel):
#     pass


#
# Entities
#
# class EntitiesPluginSettings(BasePluginSettings):
#     pass


# class EntitiesPluginSettings_v0_1_2(EntitiesPluginSettings):
#     pass


# #
# # Launcher
# #
# class LauncherPluginSettings(BasePluginSettings):
#     package_index_url: str = "unset"


# class LauncherPluginSettings_v0_1_2(LauncherPluginSettings):
#     allow_uv: bool = False
#     use_simple_index: bool = True


# class LauncherPluginSettings_v0_1_3(LauncherPluginSettings_v0_1_2):
#     allow_uv: bool = True


# class LauncherPluginSettings_v1_2_3(LauncherPluginSettings_v0_1_3):
#     auto_update: bool = True


def render_plugins_view(store: BaseStore, title: str, context_names: list[str]):

    def render_plugin_panel(plugin_name: str, plugin_require: str | None):
        required_version = None
        if plugin_require is not None:
            required_version = plugin_require.strip(
                "=="
            )  # yep, dump AF, but it's just a demo ;)
        errors = 0
        if not plugin_name:
            ui.label("Select a plugin in the list...")
            return
        # ui.label(f"{plugin_name.title()} {plugin_require}").classes("text-h5")
        plugin_settings = store.get_context_dict(
            context_names, "plugin_settings." + plugin_name
        )
        with ui.row(wrap=False).classes("w-full gap-0 p-0"):
            version_tabs = ui.tabs().props("vertical dense").classes("border")
            version_panels = (
                ui.tab_panels(version_tabs)
                .props("transition-prev=jump-up transition-next=jump-up swipeable")
                .classes("w-full border border-red-50")
            )
        for major in sorted(plugin_settings):
            for minor in sorted(plugin_settings[major]):
                for patch in sorted(plugin_settings[major][minor]):
                    version = f"{major}.{minor}.{patch}"
                    is_current = f"=={version}" == plugin_require
                    with version_tabs:
                        vtab = ui.tab(version).classes(
                            is_current and "bg-positive" or ""
                        )
                    settings_class_name = (
                        f"{plugin_name.title()}PluginSettings_v{major}_{minor}_{patch}"
                    )
                    settings_class = globals().get(settings_class_name)
                    with version_panels:
                        with ui.tab_panel(version).classes("w-full"):
                            with ui.row(align_items="center"):
                                ui.label(f"{plugin_name.title()} @ {version}").classes(
                                    "text-h6"
                                )
                                if is_current:
                                    ui.chip(
                                        "current", color="positive", icon="location_on"
                                    )
                                else:
                                    ui.button(
                                        "Set as current version",
                                        icon="swap_horizontal_circle",
                                        on_click=lambda: ui.notify(
                                            "This is just a demo ¯\\_(ツ)_/¯",
                                            type="info",
                                            position="center",
                                            close_button=True,
                                        ),
                                    ).props("dense outline")
                            # ui.space()
                            # ui.label(
                            #     f"{major}.{minor}.{patch} -> {settings_class_name} -> {settings_class}"
                            # )
                            settings_path = f"plugin_settings.{plugin_name}.{version}"
                            if settings_class is None:
                                ui.chip(
                                    "No settings model registered for this version :/",
                                    color="red-500",
                                )
                                with vtab:
                                    ui.badge("!", color="negative").props(
                                        "rounded floating"
                                    )
                                errors += 1
                                version_settings = store.get_context_flat(
                                    context_names,
                                    settings_path,
                                )
                            else:
                                try:
                                    version_settings = store.get_context(
                                        context_names,
                                        settings_class,
                                        settings_path,
                                    )
                                except Exception as err:
                                    ui.label(str(err)).classes("bg-red-500")
                                    version_settings = store.get_context_flat(
                                        context_names, settings_path
                                    )
                                else:
                                    version_settings = version_settings.model_dump()
                            # ui.label(str(version_settings))
                            for k, v in version_settings.items():
                                ui.input(k, value=str(v)).classes("w-full")
        if required_version is not None:
            version_tabs.set_value(required_version)
        return errors

    config = store.get_context(context_names, PluginRequires)
    with ui.label(title).classes("text-h5"):
        ui.tooltip(f"(We are using context {context_names} here...)")

    with ui.column().classes("w-full gap-0"):
        tabs = {}
        with (
            ui.tabs()
            .classes("w-full border")
            .props("outside-arrows dense inline-label align=left") as plugin_tabs
        ):
            for name in config.plugin_names:
                tabs[name] = ui.tab(name, icon="extension")
        with (
            ui.tab_panels(plugin_tabs)
            .props("transition-prev=jump-up transition-next=jump-up swipeable")
            .classes("w-full")
        ):
            for name in config.plugin_names:
                with ui.tab_panel(name).classes("p-0 gap-0 w-full"):
                    errors = render_plugin_panel(name, config.get_require(name))
                    if errors:
                        with tabs[name]:
                            ui.badge(str(errors), color="negative").props(
                                "rounded floating"
                            )


def render_studio_tab(store: BaseStore):
    studio_context = ["Studio"]

    studio = store.get_context(studio_context, StudioConf)
    render_plugins_view(store, f"{studio.studio_name} Settings", ["Studio"])


UI_CURRENT_PROJECT = None  # This is baaaaaad ! but it's just a demo :)
UI_CURRENT_STAGE = None  # This is a shaaaame ! but it's just a demo :)
UI_CURRENT_ENTITY = None  # This is uglyyyyyy ! but it's just a demo :)


def render_projects_tab(store: BaseStore):
    global UI_CURRENT_STAGE
    global UI_CURRENT_PROJECT

    base_project_context = ["Studio", "Plugins"]

    projects = store.get_context(base_project_context, StudioConf)

    @ui.refreshable
    def project_view():
        project_name = UI_CURRENT_PROJECT
        project_context = base_project_context + [f"Project:{project_name}"]
        stage = UI_CURRENT_STAGE
        if stage is not None:
            project_context.append(stage)
        if project_name is None:
            ui.label("Select a project first").classes("text-h5")
        else:
            context = store.get_context(project_context, ProjectContext)
            with ui.row():
                with ui.column():
                    render_plugins_view(
                        store, f"{context.project.title} Settings", project_context
                    )
                with ui.column().classes("border p-5"):
                    form_from_pydantic(context.project.settings, {})

    def on_project_select(project_code):
        global UI_CURRENT_PROJECT
        UI_CURRENT_PROJECT = project_code
        project_view.refresh()

    def on_stage_select(stage_name):
        global UI_CURRENT_STAGE
        if stage_name == "Prod":
            stage_name = None
        UI_CURRENT_STAGE = stage_name
        project_view.refresh()

    UI_CURRENT_PROJECT = projects.project_codes[0]
    UI_CURRENT_STAGE = "Prod"

    title_to_code = {}
    for project_code in projects.project_codes:
        project_title = store.get_context_flat(
            [f"Project:{project_code}"], path="project"
        ).get("title")
        title_to_code[project_code] = project_title

    with ui.row(align_items="baseline").classes("w-full"):
        ui.label("Select a project:").classes("text-h8")
        ui.select(
            title_to_code,
            value=UI_CURRENT_PROJECT,
            on_change=lambda e: on_project_select(e.value),
        ).classes("xtext-h8")
        ui.label("Select Stage:").classes("text-h8")
        ui.select(
            ["Prod", "$STAGING", "Dev:Alice", "Dev:Bob", "Alpha", "Beta"],
            value=UI_CURRENT_STAGE,
            on_change=lambda e: on_stage_select(e.value),
        ).classes("xtext-h8")
    project_view()


def _collect_entity_tree(
    parent_context_name: str, parent: dict[str, Any], store: BaseStore
):
    try:
        context = store.get_context([parent_context_name], EntityContext)
    except:
        children_names = []
    else:
        children_names = context.children_entities
    children = []
    parent["children"] = children
    for name in children_names:
        child_id = parent_context_name + "/" + name
        child = dict(id=child_id, label=name)
        children.append(child)
        _collect_entity_tree(child_id, child, store)


def render_entities_tab(store: BaseStore):
    # global UI_CURRENT_ENTITY, UI_CURRENT_PROJECT

    base_context = ["Studio", "Plugins"]

    projects = store.get_context(base_context, StudioConf)

    def on_tree_select(e):
        global UI_CURRENT_ENTITY
        UI_CURRENT_ENTITY = e.value
        entity_details.refresh()

    @ui.refreshable
    def entity_view():
        project_code = UI_CURRENT_PROJECT
        if project_code is None:
            ui.label("Select a project first").classes("text-h5")
        else:
            root_id = f"Entity:{project_code}"
            root = dict(id=root_id, label=project_code)
            _collect_entity_tree(root_id, root, store)
            with ui.row():
                with ui.column():
                    ui.tree(
                        [root],
                        label_key="label",
                        on_select=on_tree_select,
                    ).expand()
                with ui.column():
                    entity_details()

    @ui.refreshable
    def entity_details():
        project_code = UI_CURRENT_PROJECT
        entity_path = UI_CURRENT_ENTITY
        if project_code is None or entity_path is None:
            return
        else:
            entity_context = base_context + [
                f"Project:{project_code}",
                f"[{entity_path}]",
            ]
            context = store.get_context(entity_context, ProjectContext)
            with ui.row():
                with ui.column():
                    render_plugins_view(
                        store, f"{context.project.title} Settings", entity_context
                    )
                with ui.column().classes("border p-5"):
                    form_from_pydantic(context.project.settings, {})

    def on_project_select(event):
        global UI_CURRENT_PROJECT
        UI_CURRENT_PROJECT = event.value
        entity_view.refresh()

    title_to_code = {}
    for project_code in projects.project_codes:
        project_title = store.get_context_flat(
            [f"Project:{project_code}"], path="project"
        ).get("title")
        title_to_code[project_code] = project_title

    with ui.row(align_items="baseline").classes("w-full"):
        ui.label("Select a project:").classes("text-h8")
        ui.select(title_to_code, on_change=on_project_select).classes("xtext-h8")
    entity_view()


def studio_ui(store):
    with ui.tabs() as tabs:
        studio_tab = ui.tab("Studio", icon="business")
        projects_tab = ui.tab("Projects", icon="theaters")
        entities_tab = ui.tab("Entities", icon="account_tree")
    with ui.tab_panels(tabs, value=projects_tab).classes("w-full"):
        with ui.tab_panel(studio_tab):
            render_studio_tab(store)
        with ui.tab_panel(projects_tab):
            render_projects_tab(store)
        with ui.tab_panel(entities_tab):
            render_entities_tab(store)


@ui.page("/examples/cg_asset_task")
async def app_config_example():
    await header()
    await left_drawer()
    with ui.tabs().classes("w-full").props("align=left") as tabs:
        inspect_tab = ui.tab("Inspect Config")
        fronted_tab = ui.tab("Example Usage")

    with ui.tab_panels(tabs, value=fronted_tab).classes("w-full border"):
        with ui.tab_panel(inspect_tab):
            await conf_explorer(
                get_store(),
                [
                    "Studio",
                    "Plugins",
                    "Project:$PROJECT",
                    # "[Entity:{project.code}/assets/props/Table/tex]", #TODO: support this syntax
                    "[Entity:myprj/assets/props/Table/tex]",
                    "$STAGING",
                ],
            )
        with ui.tab_panel(fronted_tab):
            studio_ui(get_store())
