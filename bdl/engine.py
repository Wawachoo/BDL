from collections import namedtuple
import os
from glob import glob
import json
from urllib.parse import urlparse
import re
import importlib
import pkg_resources
import types
import bdl.logging
import bdl.progress
from bdl.exceptions import InvalidURLError
from bdl.exceptions import EngineNetworkError
from bdl.exceptions import EngineLoadError
from bdl.exceptions import EngineStructureError


by_name = {}
by_netloc = {}
NetlocRegex = namedtuple("NetlocRegex", ["url_regex", "engine_name"])


class Engine:
    """The BDL engine base class.
    """

    @staticmethod
    def get_repo_name(url, **kwargs):
        """Returns the repository name.

        Arguments:
            url (str): The repository URL.

        Returns:
            str, None: The repository name or `None`.
        """
        return None

    @staticmethod
    def is_reachable(url, **kwargs):
        """Checks if the repository exists.

        Arguments:
            url (str): The repository URL.

        Returns:
            bool: `True` if thre repository ia available, `False` oherwise.
        """
        return False

    def __init__(self, url, config, progress):
        """Initializes the engine.

        The following members are created:
            self.url: The repository URL.
            self.config: The engine configuration.
            self.name: The engine module basename
                (ex: 'bdl.engines.myengine' -> 'myengine').
            self.logger: A `bdl.logging.Logger` object.
            self.progress: A `bdl.progress.Progress` object.

        Arguments:
            url (str): The repository URL.
            config (dict): The engine configuration.
            progress (bdl.progress.Progress): Shared progress object.
        """
        self.url = url
        self.config = config
        self.name = self.__module__.split('.')[2]
        self.logger = bdl.logging.Logger("engine", self.name)
        self.progress = progress

    def pre_connect(self, **kwargs):
        """Operations to execute when connecting or cloning a repository.
        """
        pass

    def pre_update(self, **kwargs):
        """Operations to execute before updating a repository.
        """
        pass

    def count_all(self, **kwargs):
        """Returns the number of items in a repository.

        Returns:
            int: Number of items in the repository or `-1`.
        """
        return 0

    def count_new(self, last_item, last_position, **kwargs):
        """Returns the number of new items in the repository.

        Arguments:
            last_item (bdl.item.Item): The last downloaded item.
            last_position (int): The last downloaded item position in index.

        Returns:
            int: Number of new items in the repository or `-1`.
        """
        return 0

    def update_all(self, **kwargs):
        """Downloads all the available items.

        Yields:
            Instances of `bdl.item.Item`.
        """
        return [None, ]

    def update_new(self, last_item, last_position, **kwargs):
        """Downloads the newest items.

        Arguments:
            last_item (bdl.item.Item): The last downloaded item.
            last_position (int): The last downloaded item position in index.

        Yields:
            Instances of `bdl.item.Item`.
        """
        return [None, ]

    def update_selection(self, urls, **kwargs):
        """Downloads the newest items.

        Arguments:
            urls (list): List of URLs to download.

        Yields:
            Instances of `bdl.item.Item`.
        """
        return [None, ]


def _preload_localpackages():
    """Finds the available engines (legacy version).
    """
    p = os.path.join(__file__, '*')
    modules = [m for m in _glob(p)
               if os.path.basename(m) not in ["__init__.py",
                                              "__pycache__",
                                              "_by_site",
                                              "_by_name"]]
    for module in modules:
        engine_name = os.path.basename(module)
        yield (engine_name,
               "bdl.engines.{}".format(engine_name),
               os.path.join(module, "sites.json"))


def _preload_entrypoints():
    """Finds the available engines installed using setuptools entry points.
    """
    for ep in pkg_resources.iter_entry_points(group='bdl.engines'):
        yield (ep.module_name.split('.')[-1],
               ep.module_name,
               os.path.join(
                    os.path.dirname(ep.resolve().__file__), "sites.json"))


def preload():
    """Finds the available engines and references them.
    """
    def sub_preload(function):
        for engine_name, engine_module, engine_sites in function():
            by_name[engine_name] = engine_module
            try:
                with open(engine_sites, 'r') as fd:
                    config = json.load(fd)
                for netloc, url_regexes in config.items():
                    if netloc not in by_netloc:
                        by_netloc[netloc] = []
                    for url_regex in url_regexes:
                        by_netloc[netloc].append(
                            NetlocRegex(url_regex=url_regex,
                                        engine_name=engine_name))
            except Exception:
                raise
    # Clear previously loaded engine.
    by_name.clear()
    by_netloc.clear()
    sub_preload(_preload_entrypoints)
    # Find from file system is deprecated.
    # sub_preload(preload_localpackages)


def load_by_name(name):
    """Instanciate a new engine from its name (== engine package basename).

    Arguments:
        name (str): Basename of the engine to load.
            Example: basename of `bdl.engines.default` is `default`.

    Returns:
        The engine module.

    Raises (from bdl.exceptions):
        NoSuchEngineError: Requested engine doesn't exists.
        ImportEngineError: Cannot import the requested engine.
    """
    # Check if the engine exists.
    if name not in by_name:
        raise EngineLoadError(name, "Engine does not exists")
    # Load requested engine.
    try:
        return importlib.import_module(by_name[name])
    except Exception as error:
        raise EngineLoadError(name, str(error)) from error


def load_by_url(url):
    """Instanciate a new engine from the mapping URL (aka. netloc) / engines.

    Arguments:
        url (str): Return the engine which supports the specified `url`.

    Returns:
        A new engine instance.

    Raises (from bdl.exceptions):
        InvalidURLError: Format of `url` is incorrect.
        UnsupportedSiteError: No engine are available for specified `url`.
    """
    # 1: Find site name from URL.
    try:
        netloc = urlparse(url).netloc
        if len(netloc) <= 0:
            raise InvalidURLError(url) from None
    # 2: Find and return module associated with URL.
        for url_regex, engine_name in by_netloc[netloc]:
            if re.match(url_regex, url):
                return load_by_name(engine_name)
        raise EngineLoadError(None,
                              ("Unsupported URL {} for site {}")
                              .format(str(url), netloc))
    except KeyError as err:
        raise EngineLoadError(None,
                              "Unsupported site: {}".format(netloc)) from err


def validate(engine):
    """Validate an engine module.

    Arguments:
        engine (module): Engine module.

    Raises (from bdl.exceptions):
        EngineStructureError: Engine is not valid.
    """
    # Check engine module type.
    if not isinstance(engine, types.ModuleType):
        raise EngineStructureError(engine.__name__, "Engine is not a module")
    # Check engine class.
    try:
        engine_class = getattr(engine, "Engine")
        if not isinstance(engine_class, type):
            raise EngineStructureError(
                engine.__name__, "Engine class is not a class")
    except AttributeError as error:
        raise EngineStructureError(
            engine.__name__,
            "Engine module does not define an Engine class") from error
