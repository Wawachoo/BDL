import os
import json
import copy
from collections import namedtuple
import bdl
import bdl.logging
from bdl.exceptions import *


Configrule = namedtuple("Configrule", ["types", "default", "test"])


configtemplate = {
    "repo": {
        "name": Configrule(types=[str, ], default=None,
                           test=lambda x: len(x) >= 0),
        "url": Configrule(types=[str, ], default=None,
                          test=lambda x: len(x) >= 0),
        "template": Configrule(types=[str, type(None)],
                               default="{position}.{extension}",
                               test=lambda x: True)
    },
    "engine": {}
}


class Config:

    def __init__(self, path, logname=None):
        """Initializes object.

        Arguments:
            path (str): Configuration file path.
            logname (str, optional): Log name. Default is `path`.
        """
        self.__path = path
        self.__logger = bdl.logging.Logger(
            "config", logname is not None and logname or path)

    def create(self):
        """Create the configuration file.

        If the configuration file doesn't exists, a new one is created using
        the configuration template `configtemplate`.
        """
        def copy_from_template(template):
            current = {}
            for refkey, refvalue in template.items():
                if isinstance(refvalue, Configrule):
                    if refvalue.default is not None:
                        current[refkey] = refvalue.types[0](refvalue.default)
                    else:
                        current[refkey] = refvalue.types[0]()
                elif isinstance(refvalue, dict):
                    current[refkey] = copy_from_template(refvalue)
            return copy.copy(current)

        if not os.path.isfile(self.__path):
            self.__config = copy_from_template(configtemplate)
            self.commit()

    def load(self):
        """Load and validate the configuration file.

        Raises:
            ConfigContentError: The configuration file is invalid.
        """
        try:
            with open(self.__path, 'r') as fd:
                self.__config = json.load(fd)
            self.validate()
            self.__logger.info("Configuration loaded")
        except json.decoder.JSONDecodeError as error:
            raise ConfigContentError(self.__path,
                                     "Configuration format error: {}"
                                     .format(str(error))) from error

    def validate(self):
        """Validate the configuration.

        Raises:
            ConfigContentError: The configuration file is invalid.
        """
        def validate_from_template(template, current, _keypath=""):
            for refkey, refvalue in template.items():
                keypath = "{}.{}".format(_keypath, refkey)
                if refkey not in current:
                    raise ConfigContentError("Missing key {}".format(keypath))
                curvalue = current[refkey]
                if isinstance(refvalue, Configrule):
                    if type(curvalue) not in refvalue.types:
                        raise ConfigContentError(
                            self.__path,
                            "Invalid type for key {}: expecting {}, got {}"
                            .format(keypath,
                                    " or ".join([str(x) for x in
                                                 refvalue.types]),
                                    type(curvalue))
                        )
                elif isinstance(refvalue, dict):
                    validate_from_template(refvalue, current[refkey], keypath)

        validate_from_template(configtemplate, self.__config)
        self.__logger.info("Configuration validated")

    def unload(self):
        """Unload the configuration.
        """
        self.__logger.info("Configuration unloaded")

    def commit(self, path=None):
        """Validate and write the configuration.
        """
        if os.path.isdir(os.path.dirname(self.__path)):
            self.validate()
            with open(self.__path, 'w') as fd:
                json.dump(self.__config, fd, indent=2)
            self.__logger.info("Configuration commited")
        else:
            raise ConfigError("Repository doesn't exists")

    # =========================================================================
    # DIRECT ACCESS TO COMMON CONFIGURATION KEYS
    # =========================================================================

    @property
    def repo(self):
        return self.__config["repo"]

    @property
    def engine(self):
        return self.__config["engine"]

    @property
    def name(self):
        return self.__config["repo"]["name"]

    @name.setter
    def name(self, value):
        self.__config["repo"]["name"] = value

    @property
    def url(self):
        return self.__config["repo"]["url"]

    @url.setter
    def url(self, value):
        self.__config["repo"]["url"] = value

    @property
    def template(self):
        return self.__config["repo"]["template"]

    @template.setter
    def template(self, value):
        if value is not None:
            self.__config["repo"]["template"] = value
