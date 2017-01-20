import unittest
import os
import bdl


class TestLoad(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.bdl_index = bdl.index.Index(path="bdl_index_test.sqlite")

    @classmethod
    def tearDownClass(cls):
        if os.path.isfile("bdl_index_test.sqlite"):
            os.remove("bdl_index_test.sqlite")

    def tearDown(self):
        self.bdl_index.commit()
        self.bdl_index.unload()

    def test_create_index(self):
        self.bdl_index.create()
        self.bdl_index.load()

    def test_validate_index(self):
        self.bdl_index.load()
        self.bdl_index.validate()


class TestStore(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.bdl_index = bdl.index.Index(path="bdl_index_test.sqlite")
        cls.bdl_index.create()

    @classmethod
    def tearDownClass(cls):
        if os.path.isfile("bdl_index_test.sqlite"):
            os.remove("bdl_index_test.sqlite")

    def setUp(self):
        self.bdl_index.load()

    def tearDown(self):
        self.bdl_index.commit()
        self.bdl_index.unload()

    def test_store_new(self):
        # Lost of items to generate and store.
        # tuple[0]: item object
        # tuple[1]: expected filename on disk
        items = [
            (bdl.item.Item(url="http://localhost/file1.ext", filename="file1", extension="ext", storename="file1.ext"), "1.ext"),
            (bdl.item.Item(url="http://localhost/file2.ext", filename=None, extension=None, storename=None), "2.ext"),
            (bdl.item.Item(url="http://localhost/file3.ext", filename="", extension="", storename=""), "3.ext")
        ]
        # Store items.
        for item, filename in items:
            self.bdl_index.store(item)
            if not os.path.isfile(filename):
                raise Exception("Missing expected file: {}".format(filename))
            os.remove(filename)


if __name__ == '__main__':
    unittest.main()
