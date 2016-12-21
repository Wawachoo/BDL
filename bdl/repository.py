import os
import json
import sqlite3
import urllib
import copy
import bdl
from bdl.exceptions import *


def connect(url, name=None, path='.'):
    """Create a new repository and return it.

    Arguments:
        url (str): Remote repository URL.
        name (str, optional): Local repository name.
        path (str, optional): Local repository path.

    Returns:
        bdl.repository.Repository: The newly created repository.
    """
    # Validate URL & fetch repository name.
    try:
        engine_module = bdl.engine.load_by_url(url)
        bdl.engine.validate(engine_module)
        if name is None:
            name = engine_module.Engine.get_repo_name(url)
            if name is None:
                raise ConnectError(url=url,
                                   message="Cannot deduce repository name")
            name = name.replace('/', '')
    except EngineError as error:
        raise ConnectError(url=url, message=str(error)) from error
    # Create repository.
    try:
        # Create directory.
        path = path is None and '.' or path
        path = os.path.join(path, name)
        os.makedirs(os.path.join(path, ".bdl"))
        # Create configuration.
        config = bdl.config.Config(os.path.join(path, ".bdl", "config.json"))
        config.create()
        config.load()
        config.url = url
        config.name = name
        # Create index.
        index = bdl.index.Index(os.path.join(path, ".bdl", "index.sqlite"))
        index.create()
        # Connect engine.
        engine_module.Engine(url, config.engine, None).pre_connect()
        # Save config.
        config.commit()
        config.unload()
    except FileExistsError as error:
        raise ConnectError(url, "Repository already exists") from error
    # Done.
    return bdl.repository.Repository(path=path)


class Repository:

    def __init__(self, path):
        """Initializes object.

        Arguments:
            path (str): Repository path.
        """
        # Initializes paths.
        self.__path = os.path.abspath(path)
        self.__name = os.path.basename(self.__path)
        self.__bdlpath = os.path.join(self.__path, ".bdl")
        self.__configpath = os.path.join(self.__bdlpath, "config.json")
        self.__indexpath = os.path.join(self.__bdlpath, "index.sqlite")
        # Initializes logging.
        self.__logger = bdl.logging.Logger(family="repository",
                                           name=self.__name)
        # Initializes configuration.
        self.__config = bdl.config.Config(path=self.__configpath,
                                          logname=self.__name)
        # Initializes index.
        self.__index = bdl.index.Index(path=self.__indexpath,
                                       logname=self.__name)
        # Initializes progress.
        self.__progress = bdl.progress.Progress()
        # Status.
        self.__is_loaded = False
        self.__is_stop = False

    def __del__(self):
        """Uninitializes object.
        """
        try:
            self.unload()
        except Exception:
            pass

    def __assert_loaded(call):
        """Asserts the repository has been loaded.
        """
        def caller(self, *args, **kwargs):
            if self.__is_loaded is not True:
                raise RepoError(None, "Repository not loaded")
            return call(self, *args, **kwargs)
        return caller

    def __assert_reachable(call):
        """Asserts the remote repository is reachable.
        """
        def caller(self, *args, **kwargs):
            if not self.__engine.is_reachable(self.__config.url):
                raise RepoUpdateError(self.__config.name,
                                      "Repository is not reachable")
            return call(self, *args, **kwargs)
        return caller

    def load(self):
        """Load repository components.
        """
        if self.__is_loaded is True:
            return
        self.__config.load()
        self.__index.load()
        self.__index.template = self.__config.template
        self.__engine_module = bdl.engine.load_by_url(self.__config.url)
        self.__engine = self.__engine_module.Engine(self.__config.url,
                                                    self.__config.engine,
                                                    self.__progress)
        self.__is_loaded = True
        self.__logger.info("Repository loaded")

    def unload(self):
        """Unload repository components.
        """
        if self.__is_loaded is False:
            return
        self.__config.unload()
        self.__index.unload()
        self.__engine_module = None
        self.__engine = None
        self.__is_loaded = False
        self.__logger.info("Repository unloaded")

    def stop(self):
        """Stop the current action.
        """
        self.__is_stop = True

    @__assert_loaded
    def get_missing(self):
        """Returns the list of missing items.
        """
        items = []
        if self.__is_loaded:
            for item, position in self.__index.get_all():
                if (item is not None and
                not os.path.isfile(os.path.join(self.__path, item.storename))):
                    items.append((item.storename, item.url))
        return items

    def get_status(self):
        """Returns the repository status.
        """
        # Common status information.
        stats = {"reachable": self.__is_loaded,
                 "name": self.__config.name,
                 "url": self.__config.url,
                 "site": urllib.parse.urlparse(self.__config.url).netloc}
        # Advanced status information.
        if self.__is_loaded:
            stats["indexed"] = self.__index.count()
            stats["missing"] = len(self.get_missing())
            stats["new"] = self.__engine.count_new(*self.__index.get_last())
        else:
            for key in ["indexed", "missing", "new"]:
                stats[key] = 0
        # Done.
        return stats

    def get_progress(self):
        """Returns progress information.
        """
        return {
            "name": self.__progress.name,
            "percentage": self.__progress.total.percentage,
            "count": self.__progress.total.count,
            "finished": self.__progress.total.finished,
            "failed": self.__progress.total.failed
        }

    def get_config(self):
        """Returns configuration.
        """
        return {
            "repo": copy.copy(self.__config.repo),
            "engine": copy.copy(self.__config.engine)
        }

    @__assert_loaded
    @__assert_reachable
    def __update(self, up_new=False, up_missing=False, up_existing=False):
        """Update the repository.

        Arguments:
            up_new (bool, optional): Update new items.
            up_missing (bool, optional): Update missing items.
            up_existing (bool, optional): Update existing items.
        """
        # Close & re-open index.
        self.__index.unload()
        self.__index.load()
        # Prepare update.
        self.__engine.pre_update()
        # Configure to download new files.
        if up_new is True and up_existing is False:
            last_item, last_position = self.__index.get_last()
            index_update = False
            counter_func = self.__engine.count_new
            counter_args = (last_item, last_position)
            updater_func = self.__engine.update_new
            updater_args = (last_item, last_position)
        # Configure to download missing files.
        if up_missing is True and up_existing is False:
            index_update = True
            counter_func = lambda x: len(x)
            counter_args = ([url for name, url in self.get_missing()], )
            updater_func = self.__engine.update_selection
            updater_args = ([url for name, url in self.get_missing()], )
        # Configure to download all files.
        if up_existing is True:
            index_update = True
            counter_func = self.__engine.count_all
            counter_args = ()
            updater_func = self.__engine.update_all
            updater_args = ()
        # Execute.
        try:
            # Set progress.
            self.__progress.reset()
            self.__progress.name = "update"
            self.__progress.count = counter_func(*counter_args)
            # Receive and store available items.
            if self.__progress.count != 0:
                for item in updater_func(*updater_args):
                    # with self.__lock:
                    if self.__is_stop:
                        self.__logger.info("Stopped")
                        self.__is_stop = False
                        break
                    if item is not None:
                        self.__index.store(item,
                                           root=self.__path,
                                           update=index_update)
        except EngineError as error:
            raise RepoUpdateError(self.__name, str(error)) from error
        except Exception as error:
            raise RepoUpdateError(self.__name, str(error)) from error
        finally:
            self.__index.commit()
            self.__config.commit()
            self.__progress.name = None

    def update(self):
        """Update the repository (download the new items).
        """
        self.__update(up_new=True, up_missing=False, up_existing=False)
        return None

    def stash(self):
        """Re-download the deleted and the existing items.
        """
        self.__update(up_new=False, up_missing=True, up_existing=True)
        return None

    def reset(self):
        """Re-download the deleted items.
        """
        self.__update(up_new=False, up_missing=True, up_existing=False)
        return None

    def checkout(self):
        """Re-download all available items (new, missing and existing).
        """
        self.__update(up_new=True, up_missing=True, up_existing=True)
        return None

    @__assert_loaded
    @__assert_reachable
    def rename(self, template):
        """Rename indexed items and update template.

        Arguments:
            template (str): New template.
        """
        self.__config.template = template
        self.__index.rename(root=self.__path, template=template)
        self.__index.commit()
        self.__config.commit()
