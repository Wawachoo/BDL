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

    @staticmethod
    def validate(db, path):
        """Validate the database schema.

        Arguments:
            db (sqlite connection): The database to validate.
            path (str): The database path.

        Returns:
            None

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
            schema = db.execute(query_schema_bdlitems).fetchall()
            for i in range(0, len(refschema)):
                if i >= len(schema):
                    missing_columns.append(refschema[i][1])
                elif schema[i] != refschema[i]:
                    invalid_columns.append(refschema[i][1])
        except sqlite3.OperationalError as err:
            raise IndexDBSchemaError(path, "{}".format(str(err))) from err
        if len(missing_columns) > 0 or len(invalid_columns) > 0:
            raise IndexDBSchemaError(path, missing_columns, invalid_columns)

    @staticmethod
    def update(db, path, logger=None):
        """Update a database schema.

        Arguments:
            db (sqlite connection): The database to validate.
            path (str): The database path.
        """

        def _update_name():
            logger and logger.info("Trying to update table name") or None
            db.execute("""ALTER TABLE files RENAME TO bdlitems""")

        def _update_schema():
            logger and logger.info("Trying to update table schema") or None
            # Get current DB schema.
            schema = db.execute(query_schema_bdlitems).fetchall()
            # Get reference DB schema.
            refdb = sqlite3.connect(":memory:")
            refdb.execute(query_create_bdlitems)
            refschema = refdb.execute(query_schema_bdlitems).fetchall()
            # Extract schema columns definition.
            ref_columns = query_create_bdlitems.split('(')[-1].split(')')[0].replace('\n', '').split(',')
            # Update schema.
            for i in range(len(schema), len(refschema)):
                logger and logger.info("Add column {}".format(refschema[i][1])) or None
                db.execute(("ALTER TABLE bdlitems ADD COLUMN {}")
                           .format(ref_columns[i]))

        # Apply each update procedure.
        for procedure in [_update_name, _update_schema, ]:
            try:
                procedure()
                db.commit()
            except sqlite3.OperationalError as error:
                raise IndexDBError(path, str(error)) from error

    def __init__(self, path, logname=None):
        """Initializes the instance.

        Arguments:
            path (str): Path for files database.
            logname (str, optional): Index log's name. Defaults to index path.
        """
        self.db = None
        self.path = path
        self.name = os.path.basename(self.path)
        self.position = 0
        self.__template = "{position}.{extension}"
        self.logger = bdl.logging.Logger(
            "index", logname is not None and logname or path)

    @property
    def template(self):
        return self.__template

    @template.setter
    def template(self, value):
        if value is not None:
            self.__template = value

    def create(self):
        """Create the file database.
        """
        if not os.path.isfile(self.path):
            self.db = sqlite3.connect(self.path)
            self.db.execute(query_create_bdlitems)
            self.db.commit()
            self.db.close()
            self.logger.debug("Index created as {}".format(self.path))

    def load(self):
        """Load the files database.
        """
        # Load and test the database.
        self.db = sqlite3.connect(self.path)
        try:
            self.logger.debug("Validating index schema (1st try)")
            self.validate(self.db, self.path)
            retry = False
        except IndexDBSchemaError as error:
            retry = True
            self.logger.warning("Index database is outdated, trying to update")
            self.update(self.db, self.path, self.logger)
        if retry is True:
            self.logger.debug("Validating index database (2nd try)")
            self.validate(self.db, self.path)
        # Find the next position attribute.
        last_item, last_position = self.get_last()
        self.position = last_position >= 0 and last_position or 0
        self.logger.debug("Index loaded")

    def unload(self):
        """Unload (close) the files database.
        """
        if self.db is not None:
            self.db.commit()
            self.db.close()
            self.db = None
        self.logger.debug("Index unloaded")

    def commit(self):
        """Saves the files database to disk.
        """
        if self.db is not None:
            self.db.commit()
        self.logger.debug("Index commited")

    def has_item(self, item):
        """Check if `item` URL is indexed.

        Arguments:
            item (bdl.item.Item): Item to verify.

        Returns:
            Tuple with:
            * bool: `True` if item exists, `False` otherwise
            * int: Item position or -1
        """
        cursor = self.db.cursor()
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
        cursor = self.db.cursor()
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
        rows = self.db.execute(query).fetchall()
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
        # cursor = self.db.cursor()
        # cursor.execute(query_first_item)
        # db_item = cursor.fetchone()
        # if db_item is None:
        #     return (None, 0)
        # else:
        #     return (Item(*db_item[0:4], None, db_item[4], db_item[6]), db_item[6])
        queried = [i for i in self.get_queried(query_first_item)]
        return queried[0]

    def get_last(self):
        """Return the last entry in index.

        Returns:
            Tuple as:
                [0]: bdl.item.Item: Last entry in index;
                [1]: Item position in databse.
        """
        # cursor = self.db.cursor()
        # cursor.execute(query_last_item)
        # db_item = cursor.fetchone()
        # if db_item is None:
        #     return (None, 0)
        # else:
        #     return (Item(*db_item[0:4], None, db_item[4], db_item[6]), db_item[6])
        queried = [i for i in self.get_queried(query_last_item)]
        return queried[0]

    def get_all(self):
        """Returns all the entries.

        Yields:
            Tuple as:
                [0]: bdl.item.Item: An entry in index;
                [1]: Item position in databse.
        """
        # cursor = self.db.cursor()
        # cursor.execute(query_all_items)
        # db_items = cursor.fetchall()
        # for db_item in db_items:
        #     yield (Item(*db_item[0:4], None, db_item[4], db_item[6]), db_item[6])
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
        self.logger.debug("Received item: {}".format(item.url))
        # Ensure the item is valid.
        if item is None:
            self.logger.warning(
                "Ingoring item: {}: item is 'None'".format(item.url))
            return None
        # Check if item is already indexed.
        item_exists, item_position = self.has_item(item)
        # Item indexed and cannot be updated.
        if item_exists and update is not True:
            self.logger.warning(("Ignoring already indexed item: {}")
                                .format(item.url))
            return None
        # Item is indexed but can be updated.
        elif item_exists and update is True:
            cursor = self.db.cursor()
            self.logger.debug("Updating item: {}".format(item.url))
            item.storename = self.build_storename(item, item_position,
                                                  self.__template)
            cursor.execute(query_update_item, {"url": item.url,
                                               "filename": item.filename,
                                               "extension": item.extension,
                                               "storename": item.storename,
                                               "metadata": item.metadata})
        # Item not indexed in the database.
        elif not item_exists:
            cursor = self.db.cursor()
            self.logger.debug("Indexing item: {}".format(item.url))
            item.storename = self.build_storename(item, (self.position + 1),
                                                  self.__template)
            cursor.execute(query_insert_item, {"position": (self.position + 1),
                                               "url": item.url,
                                               "filename": item.filename,
                                               "extension": item.extension,
                                               "storename": item.storename,
                                               "hashed": item.hashed,
                                               "metadata": item.metadata})
            self.position += 1
        # Write item to disk.
        root = root is not None and root or '.'
        fullpath = os.path.join(root, item.storename)
        self.logger.debug("Storing item: {} -> {}".format(item.url, fullpath))
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
            storename = self.build_storename(item, position, template)
            self.logger.debug("Renaming item: {} from {} to {}"
                              .format(item.url, item.storename, storename))
            # Update item in database.
            self.db.execute(query_update_item, {"url": item.url,
                                                "filename": item.filename,
                                                "extension": item.extension,
                                                "storename": storename,
                                                "metadata": item.metadata})
            # Write item to disk.
            root = root is not None and root or '.'
            fullpath = os.path.join(root, item.storename)
            self.logger.debug("Storing item: {} as {}".format(item.url,
                                                              fullpath))
            try:
                shutil.move(os.path.join(root, item.storename),
                            os.path.join(root, storename))
            except FileNotFoundError as error:
                self.logger.warning("Missing item: {}".format(item.storename))
            except Exception:
                raise
