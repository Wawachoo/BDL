import os
import urllib
import hashlib
import time
from collections import defaultdict
import json


class Item:
    """Element downloaded from a repository.
    """

    def __init__(self, url, filename=None, extension=None, storename=None,
                 content=None, hashed=None, metadata={}, tempfile=None):
        """Initializes object.

        Arguments:
            url (str): Item URL.
            filename (str, optional): Filename, extension will be stripped.
            extension (str, optional): Extension.
            storename (str, optional): The file name on disk.
            content (data, optional): Item content.
            hashed (str, optional): The item hash.
            metadata (dict, optional): The item metadata.
            tempfile (str, optional): The item temporary file.
        """
        basename = os.path.basename(urllib.parse.urlparse(url).path)
        # Fundamental attributes.
        self.__url = url
        self.__filename = filename is not None and filename or "".join(basename.split('.')[0:-1]) or basename
        self.__extension = extension is not None and extension or basename.rfind('.') >= 0 and basename.split('.')[-1] or ''
        self.__content = content
        self.__hash = hashed
        self.__storename = storename
        self.__time = time.time()
        # Metadata.
        self.__metadata = defaultdict(str)
        self.set_metadata(metadata)
        # Temporary file.
        self.__tempfile = tempfile

    @property
    def url(self):
        return self.__url

    @property
    def filename(self):
        return self.__filename

    @property
    def extension(self):
        return self.__extension

    @property
    def content(self):
        if self.__content is not None:
            return self.__content
        else:
            return bytes()

    @property
    def hashed(self):
        if self.__hash is None:
            if isinstance(self.__content, str):
                self.__hash = hashlib.sha256(bytes(self.__content, "utf-8")).hexdigest()
            elif self.__content is not None:
                self.__hash = hashlib.sha256(self.__content).hexdigest()
            else:
                self.__hash = bytes()
        return self.__hash

    # =========================================================================
    # STORENAME MANIPULATION
    # =========================================================================

    @property
    def storename(self):
        return self.__storename

    @storename.setter
    def storename(self, value):
        self.__storename = value

    # =========================================================================
    # METADATA MANIPULATION
    # =========================================================================

    def set_metadata(self, attributes):
        """Set the item metadata.

        Arguments:
            attributes (dict): Metadata to add.
        """
        self.__metadata.update(attributes)

    def get_metadata(self):
        """Returns the internal metadata dictionnary.
        """
        return self.__metadata

    @property
    def metadata(self):
        """Returns the metadata as a JSON string.
        """
        # return ";".join(
        #     ["{}:{}".format(k, v) for k, v in self.__metadata.items()])
        return json.dumps(self.__metadata)

    # =========================================================================
    # TEMPORARY FILE MANIPULATION
    # =========================================================================

    def has_tempfile(self):
        if self.__tempfile is not None:
            return True
        return False

    @property
    def tempfile(self):
        return self.__tempfile

    @tempfile.setter
    def tempfile(self, path):
        self.__tempfile = path
