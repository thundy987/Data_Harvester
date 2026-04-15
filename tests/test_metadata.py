import os
import unittest
from pathlib import Path
from sources.windows_fs.metadata import extract_file_properties

# Absolute path to dummy_data folder
DUMMY_DATA_DIR = os.path.join(os.path.dirname(__file__), 'dummy_data')
SAMPLE_TXT = os.path.join(DUMMY_DATA_DIR, 'sample.txt')
SAMPLE_CSV = os.path.join(DUMMY_DATA_DIR, 'sample.csv')

# Known MD5 hashes for the static dummy files (computed at time of file creation)
SAMPLE_TXT_MD5 = '91766d7bb8c28ac48506f29d2f2edd9f'
SAMPLE_CSV_MD5 = '9c4941b86063d4427e9ebf42148322b5'


class TestExtractFileProperties(unittest.TestCase):

    def test_returns_tuple(self):
        result = extract_file_properties(SAMPLE_TXT)
        self.assertIsInstance(result, tuple)

    def test_returns_seven_elements(self):
        result = extract_file_properties(SAMPLE_TXT)
        self.assertEqual(len(result), 7)

    def test_parent_folder_is_path(self):
        parent_folder, *_ = extract_file_properties(SAMPLE_TXT)
        self.assertIsInstance(parent_folder, Path)

    def test_parent_folder_is_correct(self):
        parent_folder, *_ = extract_file_properties(SAMPLE_TXT)
        self.assertEqual(parent_folder, Path(DUMMY_DATA_DIR))

    def test_file_name_correct(self):
        _, file_name, *_ = extract_file_properties(SAMPLE_TXT)
        self.assertEqual(file_name, 'sample.txt')

    def test_file_extension_txt(self):
        _, _, file_extension, *_ = extract_file_properties(SAMPLE_TXT)
        self.assertEqual(file_extension, '.txt')

    def test_file_extension_csv(self):
        _, _, file_extension, *_ = extract_file_properties(SAMPLE_CSV)
        self.assertEqual(file_extension, '.csv')

    def test_create_date_is_float(self):
        _, _, _, create_date, *_ = extract_file_properties(SAMPLE_TXT)
        self.assertIsInstance(create_date, float)

    def test_last_modified_date_is_float(self):
        _, _, _, _, last_modified_date, *_ = extract_file_properties(SAMPLE_TXT)
        self.assertIsInstance(last_modified_date, float)

    def test_file_size_is_int(self):
        _, _, _, _, _, file_size, _ = extract_file_properties(SAMPLE_TXT)
        self.assertIsInstance(file_size, int)

    def test_file_size_greater_than_zero(self):
        _, _, _, _, _, file_size, _ = extract_file_properties(SAMPLE_TXT)
        self.assertGreater(file_size, 0)

    def test_md5_is_string(self):
        *_, md5_hash = extract_file_properties(SAMPLE_TXT)
        self.assertIsInstance(md5_hash, str)

    def test_md5_is_32_characters(self):
        *_, md5_hash = extract_file_properties(SAMPLE_TXT)
        self.assertEqual(len(md5_hash), 32)

    def test_md5_known_value_txt(self):
        *_, md5_hash = extract_file_properties(SAMPLE_TXT)
        self.assertEqual(md5_hash, SAMPLE_TXT_MD5)

    def test_md5_known_value_csv(self):
        *_, md5_hash = extract_file_properties(SAMPLE_CSV)
        self.assertEqual(md5_hash, SAMPLE_CSV_MD5)

    def test_md5_is_lowercase_hex(self):
        *_, md5_hash = extract_file_properties(SAMPLE_TXT)
        self.assertRegex(md5_hash, r'^[0-9a-f]{32}$')

    def test_invalid_path_raises_exception(self):
        with self.assertRaises(Exception) as ctx:
            extract_file_properties('C:/does/not/exist/fake.txt')
        self.assertIn('invalid file path', str(ctx.exception))

    def test_accepts_string_path(self):
        result = extract_file_properties(SAMPLE_TXT)
        self.assertIsNotNone(result)

    def test_accepts_path_object(self):
        result = extract_file_properties(Path(SAMPLE_TXT))
        self.assertIsNotNone(result)


if __name__ == '__main__':
    unittest.main()
