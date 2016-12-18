# General exceptions
# ==================


class BDLError(Exception):
    def __init__(self, message=""):
        super().__init__(message)
        self.message = message


class InvalidURLError(BDLError):
    def __init__(self, url):
        super().__init__(str(url))
        self.url = url


class ConnectError(BDLError):
    def __init__(self, url, message=""):
        super().__init__(message)
        self.url = url


# Repository exceptions
# =====================


class RepoError(BDLError):
    def __init__(self, repo_name, message=""):
        repo_name = repo_name is not None and repo_name or "<unknow>"
        super().__init__("{}: {}".format(repo_name, message))
        self.repo_name = repo_name


class RepoConfigError(RepoError):
    def __init__(self, repo_name, message=""):
        super().__init__(repo_name, message)


class RepoLoadError(RepoError):
    def __init__(self, repo_name, message=""):
        super().__init__(repo_name, message)


class RepoUpdateError(RepoError):
    def __init__(self, repo_name, message=""):
        super().__init__(repo_name, message)


class RepoStopError(RepoError):
    def __init__(self, repo_name, message=""):
        super().__init__(repo_name, message)


# Config exceptions
# =================


class ConfigError(BDLError):
    def __init__(self, path, message=""):
        path = path is not None and path or "<unknow>"
        super().__init__("{}: {}".format(path, message))
        self.path = path


class ConfigContentError(ConfigError):
    def __init__(self, path, message=""):
        message = "Configuration content error: {}".format(message)
        super().__init__(path, message)



# Index exceptions
# ================


class IndexDBError(BDLError):
    def __init__(self, path, message=""):
        path = path is not None and path or "<unknow>"
        super().__init__("{}: {}".format(path, message))
        self.path = path


class IndexDBSchemaError(IndexDBError):
    def __init__(self, path, missing=[], invalid=[]):
        message = "Missing columns: {}; Invalid columns: {}".format(
            ",".join(missing), ",".join(invalid))
        super().__init__(path, message)
        self.missing = missing
        self.invalid = invalid


# Engine exceptions
# =================


class EngineError(BDLError):
    """Base class for engine exceptions.
    """
    def __init__(self, engine_name, message=""):
        if engine_name is None or len(engine_name) < 1:
            engine_name = "<unknow>"
        else:
            engine_name = engine_name.split('.')[-1]
        super().__init__("{}: {}".format(engine_name, message))
        self.engine_name = engine_name


class EngineLoadError(EngineError):
    def __init__(self, engine_name, message=""):
        super().__init__(engine_name, message)


class EngineStructureError(EngineError):
    def __init__(self, engine_name, message=""):
        super().__init__(engine_name, message)


class EngineNetworkError(EngineError):
    def __init__(self, engine_name, message=""):
        super().__init__(engine_name, message)


class EngineContentError(EngineError):
    def __init__(self, engine_name, message=""):
        super().__init__(engine_name, message)


class EngineAuthError(EngineError):
    def __init__(self, engine_name, message=""):
        super().__init__(engine_name, message)


# Downloaders exceptions
# ======================


class DownloadError(BDLError):
    """Base class for downloader exceptions.
    """
    def __init__(self, url, message=""):
        super().__init__("{}: {}".format(url, message))
        self.url = url


class DownloadTimeoutError(EngineError):
    def __init__(self, url, message=""):
        super().__init__(url, message)
