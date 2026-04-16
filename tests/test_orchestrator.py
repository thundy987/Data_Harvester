import os
import unittest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch, call
from pipeline.orchestrator import (
    run_directories_pipeline,
    run_files_pipeline,
    run_pipeline,
)


def _make_mock_connection(is_directories_empty=True, is_import_files_empty=True):
    """Returns a mock db connection where table-empty checks pass by default."""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None

    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn


# ---------------------------------------------------------------------------
# run_directories_pipeline
# ---------------------------------------------------------------------------
class TestRunDirectoriesPipeline(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.sub1 = Path(self.tmp) / 'sub1'
        self.sub2 = Path(self.tmp) / 'sub1' / 'sub2'
        self.sub1.mkdir()
        self.sub2.mkdir()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _make_conn(self, empty=True):
        conn = _make_mock_connection()
        with patch('pipeline.orchestrator.is_directories_empty', return_value=empty):
            return conn

    @patch('pipeline.orchestrator.populate_directories_table')
    @patch('pipeline.orchestrator.is_directories_empty', return_value=True)
    def test_returns_dict(self, mock_empty, mock_populate):
        conn = MagicMock()
        result = run_directories_pipeline(conn, self.tmp, [self.sub1, self.sub2], 1000)
        self.assertIsInstance(result, dict)

    @patch('pipeline.orchestrator.populate_directories_table')
    @patch('pipeline.orchestrator.is_directories_empty', return_value=True)
    def test_root_in_lookup(self, mock_empty, mock_populate):
        conn = MagicMock()
        result = run_directories_pipeline(conn, self.tmp, [self.sub1, self.sub2], 1000)
        self.assertIn(self.tmp, result)

    @patch('pipeline.orchestrator.populate_directories_table')
    @patch('pipeline.orchestrator.is_directories_empty', return_value=True)
    def test_subfolders_in_lookup(self, mock_empty, mock_populate):
        conn = MagicMock()
        result = run_directories_pipeline(conn, self.tmp, [self.sub1, self.sub2], 1000)
        self.assertIn(str(self.sub1), result)
        self.assertIn(str(self.sub2), result)

    @patch('pipeline.orchestrator.populate_directories_table')
    @patch('pipeline.orchestrator.is_directories_empty', return_value=True)
    def test_populate_called_once_when_batch_not_exceeded(self, mock_empty, mock_populate):
        """root + 2 subfolders = 3 records, all flushed in a single call when batch_size > 3."""
        conn = MagicMock()
        run_directories_pipeline(conn, self.tmp, [self.sub1, self.sub2], 1000)
        self.assertEqual(mock_populate.call_count, 1)

    @patch('pipeline.orchestrator.populate_directories_table')
    @patch('pipeline.orchestrator.is_directories_empty', return_value=True)
    def test_populate_batch_contains_all_folders(self, mock_empty, mock_populate):
        """The single flushed batch should contain tuples for root + both subfolders."""
        conn = MagicMock()
        run_directories_pipeline(conn, self.tmp, [self.sub1, self.sub2], 1000)
        batch_passed = mock_populate.call_args[0][1]
        self.assertEqual(len(batch_passed), 3)

    @patch('pipeline.orchestrator.is_directories_empty', return_value=False)
    def test_raises_if_table_not_empty(self, mock_empty):
        conn = MagicMock()
        with self.assertRaises(Exception) as ctx:
            run_directories_pipeline(conn, self.tmp, [], 1000)
        self.assertIn('Directories table is not empty', str(ctx.exception))

    @patch('pipeline.orchestrator.populate_directories_table')
    @patch('pipeline.orchestrator.is_directories_empty', return_value=True)
    def test_lookup_values_are_ints(self, mock_empty, mock_populate):
        conn = MagicMock()
        result = run_directories_pipeline(conn, self.tmp, [self.sub1], 1000)
        for v in result.values():
            self.assertIsInstance(v, int)

    @patch('pipeline.orchestrator.populate_directories_table')
    @patch('pipeline.orchestrator.is_directories_empty', return_value=True)
    def test_parent_of_root_is_zero(self, mock_empty, mock_populate):
        conn = MagicMock()
        run_directories_pipeline(conn, self.tmp, [], 1000)
        # batch_list is the second positional arg; root is the first tuple in the batch
        batch = mock_populate.call_args[0][1]
        root_tuple = batch[0]
        self.assertEqual(root_tuple[0], 0)  # Parent is index 0: (Parent, ProjectID, Name)

    @patch('pipeline.orchestrator.populate_directories_table')
    @patch('pipeline.orchestrator.is_directories_empty', return_value=True)
    def test_subfolder_parent_matches_parent_project_id(self, mock_empty, mock_populate):
        conn = MagicMock()
        result = run_directories_pipeline(conn, self.tmp, [self.sub1], 1000)
        root_id = result[self.tmp]
        sub1_id = result[str(self.sub1)]
        batch = mock_populate.call_args[0][1]
        # Tuples are (Parent, ProjectID, Name)
        sub1_tuple = next(t for t in batch if t[1] == sub1_id)
        self.assertEqual(sub1_tuple[0], root_id)


# ---------------------------------------------------------------------------
# run_files_pipeline
# ---------------------------------------------------------------------------
class TestRunFilesPipeline(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.file_a = Path(self.tmp) / 'file_a.txt'
        self.file_a.write_text('hello')

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _make_lookup(self):
        return {self.tmp: 1}

    @patch('pipeline.orchestrator.populate_import_files_table')
    @patch('pipeline.orchestrator.is_import_files_empty', return_value=True)
    def test_calls_populate_for_each_file(self, mock_empty, mock_populate):
        conn = MagicMock()
        run_files_pipeline(conn, [self.file_a], self._make_lookup(), 1000)
        self.assertEqual(mock_populate.call_count, 1)

    @patch('pipeline.orchestrator.is_import_files_empty', return_value=False)
    def test_raises_if_table_not_empty(self, mock_empty):
        conn = MagicMock()
        with self.assertRaises(Exception) as ctx:
            run_files_pipeline(conn, [self.file_a], self._make_lookup(), 1000)
        self.assertIn('ImportFiles table is not empty', str(ctx.exception))

    @patch('pipeline.orchestrator.populate_import_files_table')
    @patch('pipeline.orchestrator.is_import_files_empty', return_value=True)
    def test_skips_unreadable_file_without_crashing(self, mock_empty, mock_populate):
        """A file that disappears mid-scan should be skipped, not crash the pipeline."""
        conn = MagicMock()
        ghost_file = Path(self.tmp) / 'ghost.txt'  # does not exist
        run_files_pipeline(conn, [ghost_file, self.file_a], self._make_lookup(), 1000)
        # file_a should still be processed
        self.assertEqual(mock_populate.call_count, 1)

    @patch('pipeline.orchestrator.populate_import_files_table')
    @patch('pipeline.orchestrator.is_import_files_empty', return_value=True)
    def test_empty_file_list_no_inserts(self, mock_empty, mock_populate):
        conn = MagicMock()
        run_files_pipeline(conn, [], self._make_lookup(), 1000)
        mock_populate.assert_not_called()


# ---------------------------------------------------------------------------
# run_pipeline
# ---------------------------------------------------------------------------
class TestRunPipeline(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        (Path(self.tmp) / 'file.txt').write_text('data')

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    @patch('pipeline.orchestrator.run_files_pipeline')
    @patch('pipeline.orchestrator.run_directories_pipeline')
    @patch('pipeline.orchestrator.log_activity')
    @patch('pipeline.orchestrator.walk_windows_fs')
    def test_calls_walk_with_root(self, mock_walk, mock_log, mock_dirs, mock_files):
        mock_walk.return_value = ([], [])
        mock_dirs.return_value = {}
        conn = MagicMock()
        run_pipeline(conn, self.tmp, 1000)
        mock_walk.assert_called_once_with(self.tmp)

    @patch('pipeline.orchestrator.run_files_pipeline')
    @patch('pipeline.orchestrator.run_directories_pipeline')
    @patch('pipeline.orchestrator.log_activity')
    @patch('pipeline.orchestrator.walk_windows_fs')
    def test_logs_four_activity_events(self, mock_walk, mock_log, mock_dirs, mock_files):
        mock_walk.return_value = ([], [])
        mock_dirs.return_value = {}
        conn = MagicMock()
        run_pipeline(conn, self.tmp, 1000)
        self.assertEqual(mock_log.call_count, 4)

    @patch('pipeline.orchestrator.walk_windows_fs')
    def test_raises_if_walk_fails(self, mock_walk):
        mock_walk.side_effect = Exception('scan failed')
        conn = MagicMock()
        with self.assertRaises(Exception) as ctx:
            run_pipeline(conn, self.tmp, 1000)
        self.assertIn('Directories pipeline failed', str(ctx.exception))

    @patch('pipeline.orchestrator.run_files_pipeline')
    @patch('pipeline.orchestrator.run_directories_pipeline')
    @patch('pipeline.orchestrator.log_activity')
    @patch('pipeline.orchestrator.walk_windows_fs')
    def test_raises_if_files_pipeline_fails(self, mock_walk, mock_log, mock_dirs, mock_files):
        mock_walk.return_value = ([], [])
        mock_dirs.return_value = {}
        mock_files.side_effect = Exception('files exploded')
        conn = MagicMock()
        with self.assertRaises(Exception) as ctx:
            run_pipeline(conn, self.tmp, 1000)
        self.assertIn('Files pipeline failed', str(ctx.exception))

    @patch('pipeline.orchestrator.run_files_pipeline')
    @patch('pipeline.orchestrator.run_directories_pipeline')
    @patch('pipeline.orchestrator.log_activity')
    @patch('pipeline.orchestrator.walk_windows_fs')
    def test_directories_pipeline_result_passed_to_files_pipeline(
        self, mock_walk, mock_log, mock_dirs, mock_files
    ):
        mock_walk.return_value = (['file_a'], ['folder_a'])
        mock_dirs.return_value = {'C:/root': 1}
        conn = MagicMock()
        run_pipeline(conn, self.tmp, 1000)
        _, _, lookup_passed, _ = mock_files.call_args[0]
        self.assertEqual(lookup_passed, {'C:/root': 1})


if __name__ == '__main__':
    unittest.main()
