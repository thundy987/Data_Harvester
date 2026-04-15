import unittest
from pathlib import Path
from pipeline.transformer import (
    cleanse_file_record,
    remove_white_spaces,
    find_illegal_characters_in_file_name,
    format_date,
)


class TestRemoveWhiteSpaces(unittest.TestCase):

    def test_strips_leading_spaces(self):
        self.assertEqual(remove_white_spaces('  hello'), 'hello')

    def test_strips_trailing_spaces(self):
        self.assertEqual(remove_white_spaces('hello  '), 'hello')

    def test_strips_both_ends(self):
        self.assertEqual(remove_white_spaces('  hello  '), 'hello')

    def test_no_spaces_unchanged(self):
        self.assertEqual(remove_white_spaces('hello'), 'hello')

    def test_none_returns_none(self):
        self.assertIsNone(remove_white_spaces(None))

    def test_only_spaces_returns_empty_string(self):
        self.assertEqual(remove_white_spaces('   '), '')

    def test_internal_spaces_preserved(self):
        self.assertEqual(remove_white_spaces('  hello world  '), 'hello world')


class TestFindIllegalCharactersInFileName(unittest.TestCase):

    def test_clean_name_returns_none(self):
        self.assertIsNone(find_illegal_characters_in_file_name('report.txt'))

    def test_detects_colon(self):
        result = find_illegal_characters_in_file_name('re:port.txt')
        self.assertIn(':', result)

    def test_detects_forward_slash(self):
        result = find_illegal_characters_in_file_name('re/port.txt')
        self.assertIn('/', result)

    def test_detects_backslash(self):
        result = find_illegal_characters_in_file_name('re\\port.txt')
        self.assertIn('\\', result)

    def test_detects_angle_brackets(self):
        result = find_illegal_characters_in_file_name('<report>.txt')
        self.assertIn('<', result)
        self.assertIn('>', result)

    def test_detects_pipe(self):
        result = find_illegal_characters_in_file_name('re|port.txt')
        self.assertIn('|', result)

    def test_detects_question_mark(self):
        result = find_illegal_characters_in_file_name('report?.txt')
        self.assertIn('?', result)

    def test_detects_asterisk(self):
        result = find_illegal_characters_in_file_name('report*.txt')
        self.assertIn('*', result)

    def test_detects_double_quote(self):
        result = find_illegal_characters_in_file_name('"report".txt')
        self.assertIn('"', result)

    def test_detects_trailing_period(self):
        result = find_illegal_characters_in_file_name('report.')
        self.assertIn('trailing period', result)

    def test_multiple_illegal_characters_all_returned(self):
        result = find_illegal_characters_in_file_name('<re:port>.txt')
        self.assertIn('<', result)
        self.assertIn(':', result)
        self.assertIn('>', result)

    def test_returns_list_when_illegal_found(self):
        result = find_illegal_characters_in_file_name('bad*.txt')
        self.assertIsInstance(result, list)


class TestFormatDate(unittest.TestCase):

    def test_valid_timestamp_returns_string(self):
        result = format_date(1700000000.0)
        self.assertIsInstance(result, str)

    def test_valid_timestamp_format(self):
        result = format_date(1700000000.0)
        self.assertRegex(result, r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}$')

    def test_zero_timestamp(self):
        result = format_date(0.0)
        self.assertIsInstance(result, str)

    def test_invalid_timestamp_returns_none(self):
        result = format_date(float('inf'))
        self.assertIsNone(result)

    def test_negative_timestamp_returns_none(self):
        result = format_date(-99999999999.0)
        self.assertIsNone(result)


class TestCleanseFileRecord(unittest.TestCase):

    def _make_raw_record(
        self,
        parent_folder=Path('C:/some/folder'),
        file_name='report.txt',
        file_extension='.txt',
        create_date=1700000000.0,
        last_modified_date=1700000100.0,
        file_size=1024,
        md5_hash='abc123',
    ):
        return (
            parent_folder,
            file_name,
            file_extension,
            create_date,
            last_modified_date,
            file_size,
            md5_hash,
        )

    def test_returns_dict(self):
        result = cleanse_file_record(self._make_raw_record())
        self.assertIsInstance(result, dict)

    def test_expected_keys_present(self):
        result = cleanse_file_record(self._make_raw_record())
        expected_keys = {
            'FolderPath', 'FileName', 'file_extension',
            'CreateDate', 'ModifyDate', 'FileSize', 'MD5',
        }
        self.assertEqual(set(result.keys()), expected_keys)

    def test_folder_path_is_string(self):
        result = cleanse_file_record(self._make_raw_record())
        self.assertIsInstance(result['FolderPath'], str)

    def test_file_name_whitespace_stripped(self):
        result = cleanse_file_record(self._make_raw_record(file_name='  report.txt  '))
        self.assertEqual(result['FileName'], 'report.txt')

    def test_dates_are_formatted_strings(self):
        result = cleanse_file_record(self._make_raw_record())
        self.assertRegex(result['CreateDate'], r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}$')
        self.assertRegex(result['ModifyDate'], r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}$')

    def test_file_size_preserved(self):
        result = cleanse_file_record(self._make_raw_record(file_size=2048))
        self.assertEqual(result['FileSize'], 2048)

    def test_md5_preserved(self):
        result = cleanse_file_record(self._make_raw_record(md5_hash='deadbeef'))
        self.assertEqual(result['MD5'], 'deadbeef')

    def test_extension_preserved(self):
        result = cleanse_file_record(self._make_raw_record(file_extension='.csv'))
        self.assertEqual(result['file_extension'], '.csv')

    def test_invalid_create_date_returns_none(self):
        result = cleanse_file_record(self._make_raw_record(create_date=float('inf')))
        self.assertIsNone(result['CreateDate'])

    def test_invalid_modify_date_returns_none(self):
        result = cleanse_file_record(self._make_raw_record(last_modified_date=float('inf')))
        self.assertIsNone(result['ModifyDate'])


if __name__ == '__main__':
    unittest.main()
