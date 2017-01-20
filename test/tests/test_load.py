import unittest
import bdl


class TestLoad(unittest.TestCase):

    def setUpModule():
        # Load BDL engines definition.
        bdl.engine.preload()

    def tearDownModule():
        # Re-initialize BDL engines definition.
        bdl.engine.by_name = {}
        bdl.engine.by_netloc = {}

    def test_preload_engines(self):
        return


if __name__ == '__main__':
    unittest.main()
