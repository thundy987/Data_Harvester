import unittest
from unittest.mock import MagicMock, patch, call
from db.repository import (
    is_directories_empty,
    is_import_files_empty,
    populate_directories_table,
    populate_import_files_table,
    clear_files_and_folders_tables,
    log_activity,
)


def _make_mock_connection(fetchone_return=None):
    """Helper: builds a mock pyodbc connection with a context-manager cursor."""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = fetchone_return

    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


# ---------------------------------------------------------------------------
# is_directories_empty
# ---------------------------------------------------------------------------
class TestIsDirectoriesEmpty(unittest.TestCase):

    def test_returns_true_when_table_empty(self):
        conn, _ = _make_mock_connection(fetchone_return=None)
        self.assertTrue(is_directories_empty(conn))

    def test_returns_false_when_table_has_rows(self):
        conn, _ = _make_mock_connection(fetchone_return=(1,))
        self.assertFalse(is_directories_empty(conn))

    def test_executes_correct_sql(self):
        conn, cursor = _make_mock_connection(fetchone_return=None)
        is_directories_empty(conn)
        cursor.execute.assert_called_once()
        sql_called = cursor.execute.call_args[0][0]
        self.assertIn('Directories', sql_called)

    def test_raises_exception_on_db_error(self):
        conn, cursor = _make_mock_connection()
        cursor.execute.side_effect = Exception('db error')
        with self.assertRaises(Exception) as ctx:
            is_directories_empty(conn)
        self.assertIn('Error querying Directories table', str(ctx.exception))


# ---------------------------------------------------------------------------
# is_import_files_empty
# ---------------------------------------------------------------------------
class TestIsImportFilesEmpty(unittest.TestCase):

    def test_returns_true_when_table_empty(self):
        conn, _ = _make_mock_connection(fetchone_return=None)
        self.assertTrue(is_import_files_empty(conn))

    def test_returns_false_when_table_has_rows(self):
        conn, _ = _make_mock_connection(fetchone_return=(1,))
        self.assertFalse(is_import_files_empty(conn))

    def test_executes_correct_sql(self):
        conn, cursor = _make_mock_connection(fetchone_return=None)
        is_import_files_empty(conn)
        cursor.execute.assert_called_once()
        sql_called = cursor.execute.call_args[0][0]
        self.assertIn('ImportFiles', sql_called)

    def test_raises_exception_on_db_error(self):
        conn, cursor = _make_mock_connection()
        cursor.execute.side_effect = Exception('db error')
        with self.assertRaises(Exception) as ctx:
            is_import_files_empty(conn)
        self.assertIn('Error querying ImportFiles table', str(ctx.exception))


# ---------------------------------------------------------------------------
# populate_directories_table
# ---------------------------------------------------------------------------
class TestPopulateDirectoriesTable(unittest.TestCase):

    def _make_record(self):
        return {'ProjectID': 1, 'Name': 'TestFolder', 'Parent': 0}

    def test_executes_insert(self):
        conn, cursor = _make_mock_connection()
        populate_directories_table(conn, self._make_record())
        cursor.execute.assert_called_once()

    def test_commits_transaction(self):
        conn, _ = _make_mock_connection()
        populate_directories_table(conn, self._make_record())
        conn.commit.assert_called_once()

    def test_passes_correct_values(self):
        conn, cursor = _make_mock_connection()
        record = self._make_record()
        populate_directories_table(conn, record)
        _, values = cursor.execute.call_args[0]
        self.assertEqual(values, (1, 'TestFolder', 0))

    def test_raises_exception_on_db_error(self):
        conn, cursor = _make_mock_connection()
        cursor.execute.side_effect = Exception('insert failed')
        with self.assertRaises(Exception) as ctx:
            populate_directories_table(conn, self._make_record())
        self.assertIn('Error writing to Directories table', str(ctx.exception))


# ---------------------------------------------------------------------------
# populate_import_files_table
# ---------------------------------------------------------------------------
class TestPopulateImportFilesTable(unittest.TestCase):

    def _make_record(self):
        return {
            'FileName': 'report.txt',
            'DocumentID': 1,
            'ProjectID': 2,
            'ModifyDate': '2024-01-01T00:00:00.000',
            'FolderPath': 'C:/some/folder',
            'CreateDate': '2024-01-01T00:00:00.000',
            'MD5': 'abc123',
            'FileSize': 1024,
        }

    def test_executes_insert(self):
        conn, cursor = _make_mock_connection()
        populate_import_files_table(conn, self._make_record())
        cursor.execute.assert_called_once()

    def test_commits_transaction(self):
        conn, _ = _make_mock_connection()
        populate_import_files_table(conn, self._make_record())
        conn.commit.assert_called_once()

    def test_passes_correct_values(self):
        conn, cursor = _make_mock_connection()
        record = self._make_record()
        populate_import_files_table(conn, record)
        _, values = cursor.execute.call_args[0]
        self.assertEqual(values, (
            'report.txt', 1, 2,
            '2024-01-01T00:00:00.000',
            'C:/some/folder',
            '2024-01-01T00:00:00.000',
            'abc123', 1024,
        ))

    def test_raises_exception_on_db_error(self):
        conn, cursor = _make_mock_connection()
        cursor.execute.side_effect = Exception('insert failed')
        with self.assertRaises(Exception) as ctx:
            populate_import_files_table(conn, self._make_record())
        self.assertIn('Error writing to ImportFiles table', str(ctx.exception))


# ---------------------------------------------------------------------------
# clear_files_and_folders_tables
# ---------------------------------------------------------------------------
class TestClearFilesAndFoldersTables(unittest.TestCase):

    def test_executes_two_statements(self):
        conn, cursor = _make_mock_connection()
        clear_files_and_folders_tables(conn)
        self.assertEqual(cursor.execute.call_count, 2)

    def test_truncates_import_files(self):
        conn, cursor = _make_mock_connection()
        clear_files_and_folders_tables(conn)
        all_sql = ' '.join(str(c) for c in cursor.execute.call_args_list)
        self.assertIn('ImportFiles', all_sql)

    def test_truncates_directories(self):
        conn, cursor = _make_mock_connection()
        clear_files_and_folders_tables(conn)
        all_sql = ' '.join(str(c) for c in cursor.execute.call_args_list)
        self.assertIn('Directories', all_sql)

    def test_commits(self):
        conn, _ = _make_mock_connection()
        clear_files_and_folders_tables(conn)
        conn.commit.assert_called_once()

    def test_raises_exception_on_db_error(self):
        conn, cursor = _make_mock_connection()
        cursor.execute.side_effect = Exception('truncate failed')
        with self.assertRaises(Exception) as ctx:
            clear_files_and_folders_tables(conn)
        self.assertIn('Error clearing tables', str(ctx.exception))


# ---------------------------------------------------------------------------
# log_activity
# ---------------------------------------------------------------------------
class TestLogActivity(unittest.TestCase):

    def test_executes_stored_procedure(self):
        conn, cursor = _make_mock_connection()
        log_activity(conn, 'Test activity')
        cursor.execute.assert_called_once()

    def test_passes_description(self):
        conn, cursor = _make_mock_connection()
        log_activity(conn, 'Test activity')
        _, values = cursor.execute.call_args[0]
        self.assertEqual(values[0], 'Test activity')

    def test_defaults_activity_time_to_none(self):
        conn, cursor = _make_mock_connection()
        log_activity(conn, 'Test activity')
        _, values = cursor.execute.call_args[0]
        self.assertIsNone(values[1])

    def test_accepts_explicit_activity_time(self):
        from datetime import datetime
        conn, cursor = _make_mock_connection()
        ts = datetime(2024, 1, 1, 12, 0, 0)
        log_activity(conn, 'Test activity', activity_time=ts)
        _, values = cursor.execute.call_args[0]
        self.assertEqual(values[1], ts)

    def test_commits(self):
        conn, _ = _make_mock_connection()
        log_activity(conn, 'Test activity')
        conn.commit.assert_called_once()

    def test_raises_exception_on_db_error(self):
        conn, cursor = _make_mock_connection()
        cursor.execute.side_effect = Exception('proc failed')
        with self.assertRaises(Exception) as ctx:
            log_activity(conn, 'Test activity')
        self.assertIn('Error executing Activity stored procedure', str(ctx.exception))


if __name__ == '__main__':
    unittest.main()
