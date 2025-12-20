import pydantic

from tgzr.contextual_settings.stores.memory_store import MemoryStore


def test_get_context():
    test_get_context_1()
    test_get_context_2()


def test_get_context_2():
    # Create the store
    store = MemoryStore()

    # Populate values to some contexts
    store.set("BASE", "app.name", "my_app")
    store.set("BASE", "app.title", "My App")
    store.set("BASE", "app.version", "1.0")
    store.set("BASE", "team", ["Alice", "Bob"])

    store.set("DEV", "app.title", "My App (Dev)")
    store.set("DEV", "app.version", "1.0+dev1")
    store.append("DEV", "team", "Carol")

    store.set("RC", "app.title", "My App (Release Candidat)")
    store.set("RC", "app.version", "2.0rc1")
    store.remove("RC", "team", "Bob")
    store.append("RC", "team", "Dee")

    # Test single context
    base = store.get_context_dict(["BASE"])
    assert base["app"]["name"] == "my_app"
    assert base["app"]["title"] == "My App"
    assert base["team"] == ["Alice", "Bob"]

    dev = store.get_context_dict(["DEV"])
    try:
        base["app"]["name"]
    except KeyError as err:
        assert str(err) == "name"
    assert dev["app"]["title"] == "My App (Dev)"
    assert dev["team"] == ["Carol"]

    # Test multiple contexts
    conf_dev = store.get_context_dict(["BASE", "DEV"])
    assert conf_dev["app"]["name"] == "my_app"
    assert conf_dev["app"]["title"] == "My App (Dev)"
    assert conf_dev["team"] == ["Alice", "Bob", "Carol"]

    conf_rc = store.get_context_dict(["BASE", "RC"])
    assert conf_rc["app"]["name"] == "my_app"
    assert conf_rc["app"]["title"] == "My App (Release Candidat)"
    assert conf_rc["team"] == ["Alice", "Dee"]

    # Test model config
    class AppSettings(pydantic.BaseModel):
        name: str | None = None
        title: str | None = None
        version: str | None = None

    class Config(pydantic.BaseModel):
        app: AppSettings = AppSettings()
        team: list[str] = []

    config = store.get_context(["BASE", "DEV"], model_type=Config)
    assert config.app.name == "my_app"
    assert config.app.title == "My App (Dev)"
    assert config.team == ["Alice", "Bob", "Carol"]

    # Test get dict with history
    config = store.get_context_dict(["BASE", "DEV"], with_history=True)
    assert config["app"]["name"] == "my_app"
    assert config["app"]["title"] == "My App (Dev)"
    assert config["team"] == ["Alice", "Bob", "Carol"]
    # FIXME: theses should be true:
    # assert config["__history__"]["app"]["name"][0]["context_name"] == "BASE"
    # assert len(config["__history__"]["app"]["name"]) == 1
    # assert config["__history__"]["app"]["title"][0]["context_name"] == "BASE"
    # assert config["__history__"]["app"]["title"][1]["context_name"] == "DEV"

    # Test get flat, with history
    config = store.get_context_flat(["BASE", "DEV"], with_history=True)
    assert config["app.name"] == "my_app"
    assert config["app.title"] == "My App (Dev)"
    assert config["team"] == ["Alice", "Bob", "Carol"]
    assert len(config["__history__"]["app.name"]) == 1
    assert config["__history__"]["app.name"][0]["context_name"] == "BASE"
    assert config["__history__"]["app.title"][0]["context_name"] == "BASE"
    assert config["__history__"]["app.title"][1]["context_name"] == "DEV"


def test_get_context_1():
    store = MemoryStore()
    store.set("STUDIO", "name", "Best Studio Eva")
    store.set("STUDIO", "icon", "brand_icon.ico")
    store.set("STUDIO", "settings.framerate", 12)
    store.set("MyProject", "name", "My Project")
    store.set("DEV", "is_prod", False)
    store.set("DEV", "settings.framerate", "125")  # "125" will be coerced to 125
    store.set("DEV", "settings.resolution.width", 100)
    store.set("PROD", "is_prod", True)

    class ImageSize(pydantic.BaseModel):
        width: int
        height: int

    class Settings(pydantic.BaseModel):
        framerate: int = 25
        resolution: ImageSize = ImageSize(width=2048, height=1024)

    class MyConf(pydantic.BaseModel):
        icon: str | None = None
        is_prod: bool = False
        name: str | None = None
        settings: Settings = pydantic.Field(default_factory=Settings)

    studio = store.get_context(["STUDIO"], model_type=MyConf)
    assert studio.name == "Best Studio Eva"
    assert studio.icon == "brand_icon.ico"
    assert studio.is_prod == False
    assert studio.settings.framerate == 12
    assert studio.settings.resolution.width == 2048
    assert studio.settings.resolution.height == 1024

    project = store.get_context(["MyProject"], model_type=MyConf)
    assert project.name == "My Project"
    assert project.icon == None
    assert project.is_prod == False
    assert project.settings.framerate == 25
    assert project.settings.resolution.width == 2048
    assert project.settings.resolution.height == 1024

    studio_project = store.get_context(["STUDIO", "MyProject"], model_type=MyConf)
    assert studio_project.name == "My Project"
    assert studio_project.icon == "brand_icon.ico"
    assert studio_project.is_prod == False
    assert studio_project.settings.framerate == 12
    assert studio_project.settings.resolution.width == 2048
    assert studio_project.settings.resolution.height == 1024

    validated_dev = store.get_context(["DEV"], model_type=MyConf)
    assert validated_dev.name == None
    assert validated_dev.icon == None
    assert validated_dev.is_prod == False
    assert validated_dev.settings.framerate == 125
    assert validated_dev.settings.resolution.width == 100
    assert validated_dev.settings.resolution.height == 1024
