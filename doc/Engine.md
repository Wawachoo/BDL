# Engine

## What's an engine ?
An engine is a Python module designed to download files from a specific site.


## Engine structure
An engine module, for example `Default`, must respect the following hierarchy:
```
root/
|
+- setup.py                <-----. Setup script (setuptools)
+- bdl/
   +- __init__.py          <--\
   +- engines/                 \
      |                         \
      +- __init__.py       <-----. Must extend the package namespace
      +- <engine module>/       /
         |                     /
         +- __init__.py    <--/--. Must import the Engine class
         +- sites.json     <-----. Defines the supported sites
         +- engine.py      <-----. Engine class implementation
```

### Setup file: `setup.py`
The setup script must include *at least* the following components:
```python
setup(name='bdl.engines.your_engine_name',
      ...
      packages=['bdl.engines.your_engine_name', ],
      entry_points = {'bdl.engines': ['module=bdl.engines.your_engine_name']},
      package_data={"bdl.engines.your_engine_name": ["sites.json", ]},
      ...)
```

**entry_points**  
This section is used by `BDL` (the core component of the project) to find and
load the available engines. See sections *Package namespace* and
*Engine `__init__.py`*.

**package_data**  
This section must reference the file `sites.json` which contains the definition
of the sites supported by the engine. See section *Engine site configuration*.


### Package namespace: `__init__.py`
An engine package must include a `__init__.py` file at:
* `./bdl` to extend the `bdl` namespace;
* `./bdl/engines` to extend the `bdl.engines` namespace;
This is a typicall init file structure:
```python
from pkgutil import extend_path
__path__ = extend_path(__path__, __name__)
```


### Engine `__init__.py`
The engine init file must extend the namespace `bdl.engines.your_engine_name`,
and import the `Engine` class. This is a typicall init file structure:
```python
from .engine import Engine
from pkgutil import extend_path
__path__ = extend_path(__path__, __name__)
```


### Engine sites configuration `sites.json`
Each engine module must include a file named `sites.json` which defines the
sites the module can handle. Example:
```json
{
  "default.local": [
    "http(s){0,1}://default.local/([^/]*)$",
    "http(s){0,1}://default.local/repo/([^/]*)$"
  ]
}
```
Each element (ex: `default.local`) must be a site name (==. *netloc*),
and its value must be a list of regular expressions defining the valid URLs.



## The `Engine` class
An engine class must inherit from the base class `bdl.engine.Engine`. For more
details about the engine API, look at `bdl/engine.py`.
