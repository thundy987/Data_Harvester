import os
import unittest
import tempfile
from pathlib import Path
from sources.windows_fs.scanner import walk_windows_fs


class TestWalkWindowsFs(unittest.TestCase):

    def setUp(self):
        """Create a temporary directory tree for each test."""
        self.tmp = tempfile.mkdtemp()

        # Structure:
        #   tmp/
        #     file_a.txt
        #     subdir/
        #       file_b.csv
        #       nested/
        #         file_c.xml

        open(os.path.join(self.tmp, 'file_a.txt'), 'w').close()

        subdir = os.path.join(self.tmp, 'subdir')
        os.makedirs(subdir)
        open(os.path.join(subdir, 'file_b.csv'), 'w').close()

        nested = os.path.join(subdir, 'nested')
        os.makedirs(nested)
        open(os.path.join(nested, 'file_c.xml'), 'w').close()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_returns_tuple_of_two_lists(self):
        result = walk_windows_fs(self.tmp)
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)
        file_list, folder_list = result
        self.assertIsInstance(file_list, list)
        self.assertIsInstance(folder_list, list)

    def test_correct_file_count(self):
        file_list, _ = walk_windows_fs(self.tmp)
        self.assertEqual(len(file_list), 3)

    def test_correct_folder_count(self):
        _, folder_list = walk_windows_fs(self.tmp)
        self.assertEqual(len(folder_list), 2)

    def test_file_list_contains_path_objects(self):
        file_list, _ = walk_windows_fs(self.tmp)
        for f in file_list:
            self.assertIsInstance(f, Path)

    def test_folder_list_contains_path_objects(self):
        _, folder_list = walk_windows_fs(self.tmp)
        for f in folder_list:
            self.assertIsInstance(f, Path)

    def test_all_items_in_file_list_are_files(self):
        file_list, _ = walk_windows_fs(self.tmp)
        for f in file_list:
            self.assertTrue(f.is_file(), f'{f} is not a file')

    def test_all_items_in_folder_list_are_dirs(self):
        _, folder_list = walk_windows_fs(self.tmp)
        for f in folder_list:
            self.assertTrue(f.is_dir(), f'{f} is not a directory')

    def test_known_files_present(self):
        file_list, _ = walk_windows_fs(self.tmp)
        names = [f.name for f in file_list]
        self.assertIn('file_a.txt', names)
        self.assertIn('file_b.csv', names)
        self.assertIn('file_c.xml', names)

    def test_known_folders_present(self):
        _, folder_list = walk_windows_fs(self.tmp)
        names = [f.name for f in folder_list]
        self.assertIn('subdir', names)
        self.assertIn('nested', names)

    def test_empty_directory(self):
        empty_dir = os.path.join(self.tmp, 'empty')
        os.makedirs(empty_dir)
        file_list, folder_list = walk_windows_fs(empty_dir)
        self.assertEqual(file_list, [])
        self.assertEqual(folder_list, [])

    def test_invalid_path_raises_exception(self):
        with self.assertRaises(Exception) as ctx:
            walk_windows_fs('C:/this/path/does/not/exist/xyz')
        self.assertIn('invalid root path', str(ctx.exception))

    def test_root_itself_not_in_folder_list(self):
        """The root directory should not appear in the returned folder list."""
        _, folder_list = walk_windows_fs(self.tmp)
        folder_paths = [str(f) for f in folder_list]
        self.assertNotIn(self.tmp, folder_paths)


if __name__ == '__main__':
    unittest.main()
