import os
import sqlite3
from collections import defaultdict
import json
import shutil
from bdl.item import Item
import bdl.logging
from bdl.exceptions import IndexDBError
from bdl.exceptions import IndexDBSchemaError


# Creates the 'bdlitems' table.
query_create_bdlitems = """CREATE TABLE bdlitems
                           (position INTEGER PRIMARY KEY DEFAULT 0,
                            url TEXT DEFAUTL NULL,
                            filename TEXT DEFAULT NULL,
                            extension TEXT DEFAULT NULL,
                            storename TEXT DEFAULT NULL,
                            hashed TEXT DEFAULT NULL,
                            metadata TEXT DEFAULT NULL)"""

# Returns the 'bdlitems' table schema.
query_schema_bdlitems = """pragma table_info("bdlitems")"""

# Returns the item with url ':url'.
query_has_item = """SELECT position FROM bdlitems WHERE url == :url"""

# Returns the number of items.
query_count_items = """SELECT COUNT(*) FROM bdlitems"""

# Returns all items.
query_all_items = """SELECT url, filename, extension, storename, hashed,
                            metadata, position
                     FROM bdlitems
                     ORDER BY position"""

# Returns first item.
query_first_item = """SELECT url, filename, extension, storename, hashed,
                             metadata, position
                      FROM bdlitems
                      ORDER BY position
                      ASC LIMIT 1"""

# Returns last item.
query_last_item = """SELECT url, filename, extension, storename, hashed,
                            metadata, position
                     FROM bdlitems
                     ORDER BY position
                     DESC LIMIT 1"""

# Insert a new item.
query_insert_item = """INSERT INTO bdlitems
                        VALUES (:position, :url, :filename, :extension,
                                :storename, :hashed, :metadata)"""

# Update an existing item.
query_update_item = """UPDATE bdlitems
                        SET filename = :filename,
                            extension = :extension,
                            storename = :storename,
                            metadata = :metadata
                        WHERE url == :url"""


class Index:

    def __init__(self, path, logname=None, template=None):
        """Initializes the instance.

        Arguments:
            path (str): Database file path.
            logname (str, optional): Log name. Default is `path`.
            template (str, optional): Storename template.
        """
        self.__db = None
        self.__path = path
        self.__position = 0
        self.__template = template is not None and template or ("{position}."
                                                                "{extension}")
        self.__logger = bdl.logging.Logger(
            "index", logname is not None and logname or path)

    @property
    def template(self):
        return self.__template

    @template.setter
    def template(self, value):
        if value is not None and len(value) > 0:
            self.__template = value

    def create(self):
        """Create the files database.
        """
        if not os.path.isfile(self.__path):
            self.__db = sqlite3.connect(self.__path)
            self.__db.execute(query_create_bdlitems)
            self.__db.commit()
            self.__db.close()
            self.__logger.debug("Index created as {}".format(self.__path))

    def load(self):
        """Load the files database.
        """
        # Load and validate the database.
        self.__db = sqlite3.connect(self.__path, check_same_thread=False)
        self.validate()
        # Find the next position attribute.
        last_item, last_position = self.get_last()
        self.__position = last_position >= 0 and last_position or 0
        self.__logger.debug("Index loaded")

    def validate(self):
        """Validate the database schema.

        Raises:
            IndexSchemaError: The database schema is invalid.
            IndexVersionError: The repository index version is not supported.
        """
        # Create an in-memory database with the default schema.
        refdb = sqlite3.connect(":memory:")
        refdb.execute(query_create_bdlitems)
        refschema = refdb.execute(query_schema_bdlitems).fetchall()
        # List of missing and invalid columns.
        missing_columns = []
        invalid_columns = []
        # Check each column.
        try:
            schema = self.__db.execute(query_schema_bdlitems).fetchall()
            for i in range(0, len(refschema)):
                if i >= len(schema):
                    missing_columns.append(refschema[i][1])
                elif schema[i] != refschema[i]:
                    invalid_columns.append(refschema[i][1])
        except sqlite3.OperationalError as err:
            raise IndexDBSchemaError(self.__path, str(err)) from err
        if len(missing_columns) > 0 or len(invalid_columns) > 0:
            raise IndexDBSchemaError(self.__path, missing_columns,
                                     invalid_columns)

    def unload(self):
        """Unload (close) the files database.
        """
        if self.__db is not None:
            self.__db.commit()
            self.__db.close()
            self.__db = None
        self.__logger.debug("Index unloaded")

    def commit(self):
        """Saves the files database to disk.
        """
        if self.__db is not None:
            self.__db.commit()
        self.__logger.debug("Index commited")

    def has_item(self, item):
        """Check if `item` URL is indexed.

        Arguments:
            item (bdl.item.Item): Item to verify.

        Returns:
            Tuple with:
            * bool: `True` if item exists, `False` otherwise
            * int: Item position or -1
        """
        cursor = self.__db.cursor()
        cursor.execute(query_has_item, {"url": item.url})
        result = cursor.fetchone()
        if result is not None:
            return (True, result[0])
        return (False, -1)

    def __contains__(self, item):
        """`in` operator, alias for `has_item`.
        """
        return self.has_item(item)

    def count(self):
        """Returns the number of items in the database.

        Returns:
            int: Number of indexed items.
        """
        cursor = self.__db.cursor()
        cursor.execute(query_count_items)
        return int(cursor.fetchone()[0])

    def __len__(self):
        """`len` magic method, alias for `count`.
        """
        return self.count()

    def get_queried(self, query):
        """Yields items instances returned by the specified query.

        Arguments:
            query (str): Query to execute.

        Yields:
            Tuple as:
                [0]: bdl.item.Item: Item instance;
                [1]: Item position in databse.
        """
        rows = self.__db.execute(query).fetchall()
        if len(rows) < 1:
            yield (None, 0)
        else:
            for row in rows:
                try:
                    metadata = json.loads(row[5])
                except (json.decoder.JSONDecodeError, TypeError):
                    metadata = {}
                yield (Item(*row[0:4], None, row[4], metadata),
                       row[6])

    def get_first(self):
        """Return the first entry in index and its position.

        Returns:
            Tuple as:
                [0]: bdl.item.Item: First entry in index;
                [1]: Item position in databse.
        """
        queried = [i for i in self.get_queried(query_first_item)]
        return queried[0]

    def get_last(self):
        """Return the last entry in index.

        Returns:
            Tuple as:
                [0]: bdl.item.Item: Last entry in index;
                [1]: Item position in databse.
        """
        queried = [i for i in self.get_queried(query_last_item)]
        return queried[0]

    def get_all(self):
        """Returns all the entries.

        Yields:
            Tuple as:
                [0]: bdl.item.Item: An entry in index;
                [1]: Item position in databse.
        """
        for itempos in self.get_queried(query_all_items):
            yield (itempos)

    def build_storename(self, item, position, template):
        """Returns an item storename.

        Arguments:
            item (bdl.item.Item): Valid Item instance.
            position (int): item position in repository.
            template (str): Storename template.
        """
        attributes = defaultdict(str, {"position": position,
                                       "filename": item.filename,
                                       "extension": item.extension})
        attributes.update(item.get_metadata())
        return template.format_map(attributes)

    def store(self, item, root=None, update=False):
        """Store a new item.

        Arguments:
            item (bdl.item.Item): Item to store.
            root (str, optional): Where to write the file.
            update (bool, optional): Update an existing item.

        Returns:
            Item storename as `str` or `None`
        """
        self.__logger.debug("Received item: {}".format(item.url))
        # Ensure the item is valid.
        if item is None:
            self.__logger.warning(
                "Ingoring item: {}: item is 'None'".format(item.url))
            return None
        # Check if item is already indexed.
        item_exists, item_position = self.has_item(item)
        # Item indexed and cannot be updated.
        if item_exists and update is not True:
            self.__logger.warning(("Ignoring already indexed item: {}")
                                  .format(item.url))
            return None
        # Item is indexed but can be updated.
        elif item_exists and update is True:
            cursor = self.__db.cursor()
            self.__logger.debug("Updating item: {}".format(item.url))
            item.storename = self.build_storename(item, item_position,
                                                  self.__template)
            cursor.execute(query_update_item, {"url": item.url,
                                               "filename": item.filename,
                                               "extension": item.extension,
                                               "storename": item.storename,
                                               "metadata": item.metadata})
        # Item not indexed in the database.
        elif not item_exists:
            cursor = self.__db.cursor()
            self.__logger.debug("Indexing item: {}".format(item.url))
            item.storename = self.build_storename(item, (self.__position+1),
                                                  self.__template)
            cursor.execute(query_insert_item, {"position": (self.__position+1),
                                               "url": item.url,
                                               "filename": item.filename,
                                               "extension": item.extension,
                                               "storename": item.storename,
                                               "hashed": item.hashed,
                                               "metadata": item.metadata})
            self.__position += 1
        # Write item to disk.
        root = root is not None and root or '.'
        fullpath = os.path.join(root, item.storename)
        self.__logger.debug("Storing item: {}: {}".format(item.url, fullpath))
        if item.has_tempfile:
            shutil.move(item.tempfile, fullpath)
        else:
            with open(fullpath, "wb") as fd:
                fd.write(item.content)
        # Done.
        return item.storename

    def rename(self, root=None, template=None):
        """Rename the existing items.

        Arguments:
            root (str, optional): Where to write the file.
            template (str, optional): New storename template.
        """
        template = template is not None and template or self.__template
        for item, position in self.get_all():
            if item is None:
                break
            storename = self.build_storename(item, position, template)
            self.__logger.debug("Renaming item: {} from {} to {}"
                                .format(item.url, item.storename, storename))
            # Update item in database.
            self.__db.execute(query_update_item, {"url": item.url,
                                                  "filename": item.filename,
                                                  "extension": item.extension,
                                                  "storename": storename,
                                                  "metadata": item.metadata})
            # Write item to disk.
            root = root is not None and root or '.'
            fullpath = os.path.join(root, item.storename)
            self.__logger.debug("Storing item: {} as {}".format(item.url,
                                                                fullpath))
            try:
                shutil.move(os.path.join(root, item.storename),
                            os.path.join(root, storename))
            except FileNotFoundError as error:
                self.__logger.warning("Missing item: {}"
                                      .format(item.storename))
            except Exception:
                raise
