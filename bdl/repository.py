import os
import json
import sqlite3
from collections import namedtuple
import signal
import threading
import bdl.logging
import bdl.item
import bdl.engine
import bdl.index
from bdl.exceptions import *


RepoStatus = namedtuple("RepoStatus", ["reachable",
                                       "indexed",
                                       "new",
                                       "missing"])


class Repository:

    def __init__(self, url=None, path=None, root=None, engine_name=None,
                 template=None):
        """Initializes the Repository object.

        Arguments:
            url (str, optional): Remote repository URL.
            path (str, optional): Path to local repository.
            root (str, optional): Local repository parent os.path.
            engine_name (str, optional): Forces this engine module.
            kwargs (dict, optional): Engine arguments.
        """
        # Class attributes.
        self.url = url
        self.path = path
        self.root = root is not None and root or '.'
        self.name = path is not None and os.path.basename(path) or None
        self.engine = None
        self.engine_name = engine_name
        self.engine_module = None
        self.engine_config = {}
        self.logger = None
        self.messages = []
        self.index = None
        self.template = template
        self.progress = bdl.progress.Progress()
        self.__state = {}
        # Initializes repository.
        self.__init_engine_module(url)
        self.__init_paths(url, path, root)
        self.__init_state()
        self.logger = bdl.logging.Logger("repository", self.name)
        # Signal handling.
        signal.signal(signal.SIGINT, self._stop)
        # Late logging.
        self.logger.debug("Path: {}".format(self.path))
        self.logger.debug("Name: {}".format(self.name))
        self.logger.debug("Engine module: {}".format(self.engine_module))

    # ========================================================================
    # INITIALIZERS
    # ========================================================================

    def __init_engine_module(self, url, **kwargs):
        """Find the repository engine module.
        """
        engine_loader = (None, None)
        # Find engine loader function.
        if self.engine_name is not None:
            engine_loader = (bdl.engine.load_by_name, self.engine_name)
        elif url is not None:
            engine_loader = (bdl.engine.load_by_url, url)
        # Load engine.
        if engine_loader[0] is not None:
            try:
                self.engine_module = engine_loader[0](engine_loader[1])
                bdl.engine.validate(self.engine_module)
            except EngineError as error:
                raise RepoError(self.name,
                                ("Cannot load engine module: {}")
                                .format(str(error))) from error

    def __init_paths(self, url, path, root):
        """Initializes the repository paths.
        """
        try:
            if path is not None:
                self.path = os.path.abspath(os.path.join(self.root, path))
            else:
                name = self.engine_module.Engine.get_repo_name(url)
                name = name.replace('/', '')
                self.path = os.path.abspath(os.path.join(self.root, name))
        except (EngineError, TypeError) as error:
            raise RepoError(
                self.name, "Cannot retrieve repository name") from error
        if self.path is None:
            raise RepoError(self.name, "Cannot retrieve repository name")
        self.name = os.path.basename(self.path)
        self.bdlpath = os.path.join(self.path, ".bdl")
        self.configpath = os.path.join(self.bdlpath, "config.json")
        self.indexpath = os.path.join(self.bdlpath, "index.sqlite")

    def __init_state(self):
        """Initializes status attributes.
        """
        self.__state["stop"] = False

    # ========================================================================
    # CONFIGURATION & INDEX & ENGINE MANIPULATION
    # ========================================================================

    def __save_config(self):
        """Commit the repository configuration.
        """
        self.logger.debug("Writing configuration: {}".format(self.configpath))
        with open(self.configpath, 'w') as fd:
            json.dump(
                {"repo": {"url": self.url,
                          "template": self.template},
                 "engine": {**self.engine_config}},
                fd,
                indent=2)

    def __load_config(self):
        """Load the repository configuration.
        """
        self.logger.info("Loading configuration: {}".format(self.configpath))
        try:
            with open(self.configpath, 'r') as fd:
                config = json.load(fd)
                self.url = config["repo"]["url"]
                template = config["repo"].get("template", None)
                self.template = template
                self.engine_config = config["engine"]
                self.status
        except FileNotFoundError as error:
            raise RepoConfigError(self.name,
                                  ("Configuration file does not exists: {}")
                                  .format(self.configpath)) from error
        except KeyError as error:
            raise RepoConfigError(self.name,
                                  ("Missing configuration attribute: {}")
                                  .format(str(error))) from error
        except json.decoder.JSONDecodeError as error:
            raise RepoConfigError(self.name,
                                  ("Configuration format error: {}")
                                  .format(str(error))) from error

    def __load_index(self, force=False):
        """Load the the repository index.
        """
        if self.index is None or force is True:
            self.logger.debug("Loading index: {}".format(self.indexpath))
            self.index = bdl.index.Index(self.indexpath, logname=self.name)
            self.index.load()

    def __load_engine(self, force=False):
        """Load the repository engine.
        """
        if self.engine_module is None or force is True:
            self.logger.debug("Loading engine module")
            self.__init_engine_module(self.url)
        if self.engine is None or force is True:
            self.logger.debug(("Instanciating new engine: {}")
                              .format(self.engine_module.__name__))
            self.engine = self.engine_module.Engine(self.url,
                                                    self.engine_config,
                                                    self.progress)

    # ========================================================================
    # INTERNAL API
    # ========================================================================

    def _stop(self, *args):
        """Set all state flags to stop.
        """
        self.__state["stop"] = True
        raise RepoStopError(self.name, "Stopped")

    def _load_components(self, force=False):
        """Load the repository configuration, engine and index.
        """
        if self.path is None or len(self.path) < 1:
            raise RepoLoadError(None, "Missing path")
        if not os.path.isdir(self.path):
            raise RepoLoadError(os.path.normpath(self.path),
                                "Repository does not exists")
        self.__load_config()
        self.__load_index(force)
        self.__load_engine(force)

    def _connect(self):
        """Connect to the remote repository.
        """
        # Checking attributes required for connection.
        for attribute, message in [
            (self.url, "Missing URL"),
            (self.path, "Missing path"),
            (self.engine_module, "Missing engine module")
        ]:
            if attribute is None:
                raise RepoConnectError(self.name, message)
        # Create repository.
        try:
            os.makedirs(self.bdlpath)
        except FileExistsError as error:
            raise RepoConnectError(self.name,
                                   "Repository already exists") from error
        # Engine connection.
        self.__load_engine()
        self.engine.pre_connect()
        # Create configuration file and index.
        self.__save_config()
        self.index = bdl.index.Index(self.indexpath)
        self.index.create()

    def _status(self):
        """Returns the repository status.
        """
        self._load_components()
        last_item, last_position = self.index.get_last()
        return RepoStatus(
            reachable=self.engine_module.Engine.is_reachable(self.url),
            indexed=self.index.count(),
            new=self.engine.count_new(last_item, last_position),
            missing=len(self._missing()))

    def _missing(self):
        """Returns the list of missing files.
        """
        self._load_components()
        missing = []
        for item, position in self.index.get_all():
            if (item is not None and
                not os.path.isfile(os.path.join(self.path, item.storename))):
                missing.append((item.storename, item.url))
        return missing

    def _update(self, up_new=False, up_missing=False, up_existing=False):
        """Update the repository.

        Arguments:
            up_new (bool, optional): Update new items.
            up_missing (bool, optional): Update missing items.
            up_existing (bool, optional): Update existing items.
            template (str, optional): Items store name template.
        """
        self._load_components()
        if not self.engine.is_reachable(self.url):
            raise RepoUpdateError(self.name, "Repository is not reachable")
        self.engine.pre_update()
        stored = []
        ignored = []
        # Download new files.
        if up_new is True and up_existing is False:
            self.logger.info("Update mode: new items")
            last_item, last_position = self.index.get_last()
            index_update = False
            counter_func = self.engine.count_new
            counter_args = (last_item, last_position)
            updater_func = self.engine.update_new
            updater_args = (last_item, last_position)
        # Download missing files.
        if up_missing is True and up_existing is False:
            self.logger.info("Update mode: missing items")
            index_update = True
            counter_func = lambda x: len(x)
            counter_args = ([url for name, url in self._missing()], )
            updater_func = self.engine.update_selection
            updater_args = ([url for name, url in self._missing()], )
        # Download all files.
        if up_existing is True:
            self.logger.info("Update mode: all items")
            index_update = True
            counter_func = self.engine.count_all
            counter_args = ()
            updater_func = self.engine.update_all
            updater_args = ()
        # Execute.
        try:
            # Set index storename template.
            self.index.template = self.template
            # Get the number of items to download.
            self.progress.count = counter_func(*counter_args)
            self.logger.debug("Update count: {}".format(self.progress.count))
            # Receive and store available items.
            if self.progress.count != 0:
                for item in updater_func(*updater_args):
                    # Check if the calling thread didn't stop.
                    if self.__state["stop"]:
                        self.logger.info("Stopping: `__state` set to `stop`")
                        break
                    # Store item.
                    if item is not None:
                        self.index.store(item, root=self.path,
                                         update=index_update)
                    else:
                        self.logger.warning("Received `None` item")
        except EngineError as error:
            raise RepoUpdateError(self.name, str(error)) from error
        except RepoStopError:
            self.logger.debug("Stopping: received `RepoStopError` exception")
        except Exception as error:
            raise RepoUpdateError(self.name, str(error)) from error
        finally:
            self.index.commit()
            self.__save_config()

    def _rename(self, template):
        """Rename indexed items and update template.

        Arguments:
            template (str): New template.
        """
        self._load_components()
        self.template = template
        self.index.rename(root=self.path, template=template)
        self.index.commit()
        self.__save_config()

    # ========================================================================
    # PUBLIC API
    # ========================================================================

    def connect(self):
        """Connect to the remote repository.
        """
        self._connect()

    def clone(self):
        """Clone the remote repository (connect & update).
        """
        self._connect()
        self._load_components(force=True)
        return self._update(up_new=True, up_missing=False, up_existing=False)

    def update(self):
        """Update the repository (download the new items).
        """
        return self._update(up_new=True, up_missing=False, up_existing=False)

    def stash(self):
        """Re-download the deleted and the existing items.
        """
        return self._update(up_new=False, up_missing=True, up_existing=True)

    def reset(self):
        """Re-download the deleted items.
        """
        return self._update(up_new=False, up_missing=True, up_existing=False)

    def checkout(self):
        """Re-download all available items (new, missing and existing).
        """
        return self._update(up_new=True, up_missing=True, up_existing=True)

    def status(self):
        """Returns the repository satus.
        """
        return self._status()

    def diff(self):
        """Return a list of missing items (absent from filesystem).
        """
        return self._missing()

    def rename(self, template):
        """Rename all indexed items using the new template.
        """
        return self._rename(template)
