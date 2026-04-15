import unittest
from utils.id_generator import create_project_id, create_document_id


class TestIdGenerator(unittest.TestCase):

    def test_create_project_id_returns_int(self):
        result = create_project_id()
        self.assertIsInstance(result, int)

    def test_create_document_id_returns_int(self):
        result = create_document_id()
        self.assertIsInstance(result, int)

    def test_create_project_id_increments(self):
        first = create_project_id()
        second = create_project_id()
        self.assertEqual(second, first + 1)

    def test_create_document_id_increments(self):
        first = create_document_id()
        second = create_document_id()
        self.assertEqual(second, first + 1)

    def test_project_and_document_counters_are_independent(self):
        """Advancing one counter should not affect the other."""
        p1 = create_project_id()
        d1 = create_document_id()
        p2 = create_project_id()
        d2 = create_document_id()
        self.assertEqual(p2, p1 + 1)
        self.assertEqual(d2, d1 + 1)


if __name__ == '__main__':
    unittest.main()
