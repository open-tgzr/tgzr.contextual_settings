# tgzr.contextual_settigs

Manage settings values in layers of operations.

## Install

`pip install tgzr.contextual_settigs`

If you want to use the demo GUI:
`pip install tgzr.contextual_settigs[demo]`
See [Running the Demo](#running-the-demo)

## Usage

There will be more store types in the future (FileStore, RESTStore, SqlStore, ...)
But there's only MemoryStore for now.

Also, you'll be able to provide your own store implementations.

### MemoryStore

The MemoryStore stores the config in memory.
You need to populate it yourself.

```python
from tgzr.contextual_settigs.stores.memory_store import MemoryStore

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

```

### Resolving Config

#### As Nested Dicts
Once the store is populated, you can retrieve the config for a given context:
```python

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

```

Or for a list of stacked contexts:
```python
conf_dev = store.get_context_dict(["BASE", "DEV"])
assert conf_dev["app"]["name"] == "my_app"
assert conf_dev["app"]["title"] == "My App (Dev)"
assert conf_dev["team"] == ["Alice", "Bob", "Carol"]

conf_rc = store.get_context_dict(["BASE", "RC"])
assert conf_rc["app"]["name"] == "my_app"
assert conf_rc["app"]["title"] == "My App (Release Candidat)"
assert conf_rc["team"] == ["Alice", "Dee"]

```
#### With History

if you want to inspect how the value was built, you can set the 
`with_history` arg to `True`.
The returned dict will contain a `__history__` key containing a dict
with the same keys as previously, but each value for these key is a list
of dict depicting the operation affecting the value:
```python
config = store.get_context_dict(["BASE", "DEV"], with_history=True)
assert config["app"]["name"] == "my_app"
assert config["app"]["title"] == "My App (Dev)"
assert config["team"] == ["Alice", "Bob", "Carol"]
assert config["__history__"]["app"].get("name") is None
assert config["__history__"]["app"]["title"][0]["context_name"] == "DEV"
assert config["__history__"]["app"]["title"][0]["op_name"] == "Set"
```
The history list contain dicts with these keys:
- `context_name`: the name of the context affecting the value
- `op_name`: the name of the operation affecting the value
- `op`: a string representation of the operation affecting the value

#### As Flat Dict

If you prefer to access your value without nested dict, you can use
`get_context_flat()` (with or without history):
```python
config = store.get_context_flat(["BASE", "DEV"])
assert config["app.name"] == "my_app"
assert config["app.title"] == "My App (Dev)"
assert config["team"] == ["Alice", "Bob", "Carol"]
assert config["__history__"].get("app.name") is None
assert config["__history__"]["app.title"][0]["context_name"] == "DEV"
assert config["__history__"]["app.title"][0]["op_name"] == "Set"
```

#### As Pydantic Models

You can get a config as an instance of a `pydantic.BaseModel`.
This is usefull to coerce / validate the schema of the generated config, and
also provide you with ✨code completion✨:
```python
import pydantic

class AppSettings(pydantic.BaseModel):
    name: str | None = None
    title: str | None = None
    version: str | None = None

class Config(pydantic.BaseModel):
    app: AppSettings = AppSettings()
    team: list[str] = []

config = store.get_context(["BASE", "DEV"], Config)
assert config.app.name == "my_app"
assert config.app.title == "My App (Dev)"
assert config.team == ["Alice", "Bob", "Carol"]
```


    Note: 
    You cannot request the history when getting the context as a pydandic model.

### Context name expansion

#### Environment Variables

You can use environment variable in the context names:

```python
import os

os.environ['STAGE'] = 'PROD'

config_1 = store.get_context_dict(['defaults', '$STAGE'])
config_2 = store.get_context_dict(['defaults', 'PROD'])
assert config_1 == config_2

```

You can even nest environment variables:

```python
import os

os.environ['FirstName'] = 'Guido'
os.environ['LastName'] = 'van Rossum'
os.environ['FullName'] = '$FirstName $LastName'

config_1 = store.get_context_dict(['$FullName'])
config_2 = store.get_context_dict(['Guido van Rossum'])
assert config_1 == config_2

```

#### Expand Path

When you manage context for a hierarchy of things, you often need
to specify a context per level of that hierarchy.
For example, with a hierarchy like:

    '$project_name/teams/$team_name'

You'll probably want to use context names like:

    ['@project_name', '$project_name/teams', '$project_name/teams/$team_name']

to cumulate all the value set at each level of the hierarchy.

To generate this list, you can use the `Expand Path` notation.
When a context name is enclosed with square brackets, the name will be 
treated as the path you want to expand:
```python

config_1 = store.get_context_dict(['Base', '[$PROJ/Teams/$Team]', 'Last'])
config_2 = store.get_context_dict(
    ['Base', '$PROJ', '$PROJ/Teams', '$PROJ/Teams/$Team', 'Last']
)
assert config_1 == config_2
```

(Of course, environement variable will be reduced too)

## Running the Demo

To run the Demo GUI, you must install with the demo extra:
```
pip install tgzr.contextual_settigs[demo]
```

And then, you can launch the demo with:
```
uv run tgzr_contextual_settings_demo
```
or, if you want to pass arguments to faspapi:
```
uv run -p 3.13 --extra demo fastapi dev ./src/tgzr/contextual_settigs/demo/app.py
```
This will open a browser window with the Demo GUI.

### With docker

If you fancy it, you can build a docker image and run it.

From the root of the repository:
```
docker build -t tgzr-contextual_settigs-demo .
docker run -d --name contextual_settigs_demo -p 8083:8080 tgzr-contextual_settigs-demo
```

## Implementation Checklist

- [X] MemoryStore
- [X] Basic set of operators
- [X] Get context as dict
- [X] Get context as flat dict
- [X] Get context as pydantic model
- [X] Support env var in context names
- [X] Support path expansion in context names
- [X] Get context with history
- [ ] Rename *store -> *Conf ?
- [X] Support context info (color, type, ...)
- [~] web UI
- [X] Update context with dict
- [X] Update context with flat dict
- [X] Update context with pydantic model
- [~] Base models for collection of items
- [ ] Validate context with pydantic model, report all error (for gui/inspection)
- [ ] Refactor tests wiht pytest
- [ ] test store.get_context* with path
- [ ] Rest Service
- [ ] desktop UI
- [ ] Fancy operators
- [ ] FileStore
- [ ] SqlStore
- [ ] RestStore
- [ ] Support string templating in context name
- [ ] Support env var in values
- [ ] Support string templating in values
