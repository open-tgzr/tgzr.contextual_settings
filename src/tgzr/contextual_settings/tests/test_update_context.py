import pydantic

from tgzr.contextual_settings.stores.memory_store import MemoryStore


def test_update_context():
    test_update_context_flat()
    test_update_context_dict()
    test_update_context_model()


def test_update_context_flat():
    # FIXME: what should happen if the key is a dict ?!?
    # for now it sets the dict as value, and it creates a lot of issues
    # should we auto update_context_dict() for dict value?
    # or should we raise an exception if a value is a dict?

    flat_conf = {
        "path.to.value1": "path to value #1",
        "path.to.value2": "path to value #2",
        "path.with_list.int_list": [1, 2, 3],
        "path.with_list.str_list": ["a", "b", "c"],
    }

    store = MemoryStore()
    store.update_context_flat("Config", flat_conf)

    config = store.get_context_flat(["Config"])
    assert config == flat_conf

    path_with_config = store.get_context_flat(["Config"], path="path.with_list")
    assert path_with_config == dict(int_list=[1, 2, 3], str_list=["a", "b", "c"])


def test_update_context_dict():

    deep_conf = dict(
        path=dict(
            to=dict(
                value1="path to value #1",
                value2="path to value #2",
            ),
            with_list=dict(
                int_list=[1, 2, 3],
                str_list=["a", "b", "c"],
            ),
        )
    )

    store = MemoryStore()
    store.update_context_dict("Config", deep_conf)

    config = store.get_context_dict(["Config"])
    # print(config)
    assert config == deep_conf

    path_with_config = store.get_context_dict(["Config"], path="path.with_list")
    # print(path_with_config)
    assert path_with_config == deep_conf["path"]["with_list"]


def test_update_context_model():
    class ToModel(pydantic.BaseModel):
        value1: str = ""
        value2: str = ""

    class WithListModel(pydantic.BaseModel):
        int_list: list[int] = []
        str_list: list[str] = []

    class PathModel(pydantic.BaseModel):
        to: ToModel = ToModel()
        with_list: WithListModel = WithListModel()

    class ConfModel(pydantic.BaseModel):
        path: PathModel = PathModel()

    my_conf = ConfModel()
    my_conf.path.to.value1 = "path to value #1"
    my_conf.path.to.value2 = "path to value #2"
    my_conf.path.with_list.int_list = [1, 2, 3]
    my_conf.path.with_list.str_list = ["a", "b", "c"]

    store = MemoryStore()
    store.update_context("Config", my_conf)

    config = store.get_context(["Config"], ConfModel)
    # print(config)
    assert config == my_conf

    path_with_config = store.get_context(
        ["Config"], WithListModel, path="path.with_list"
    )
    # print(path_with_config)
    assert path_with_config == my_conf.path.with_list
