from datetime import datetime
from unittest.mock import MagicMock

import pytest

from db.repository import (
    clear_files_and_folders_tables,
    is_directories_empty,
    is_import_files_empty,
    log_activity,
    populate_directories_table,
    populate_import_files_table,
)


def test_is_directories_empty_returns_true_when_empty():
    """Validates the following condition for is_directories_empty:
    1. Returns True when fetchone() returns None (table is empty).

    Args:
        mock_connection (MagicMock): Mock db connection with cursor returning no rows.
    """
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    # user __enter__ to return fake cursor, since cursor() is used as a `with` block.
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchone.return_value = None

    result = is_directories_empty(mock_connection)

    assert result is True
    mock_cursor.execute.assert_called_once_with('SELECT TOP(1) 1 FROM Directories')


def test_is_directories_empty_returns_false_when_not_empty():
    """Validates the following condition for is_directories_empty:
    1. Returns False when fetchone() returns a row (table is not empty).

    Args:
        mock_connection (MagicMock): Mock db connection with cursor returning a row.
    """
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchone.return_value = (1,)

    result = is_directories_empty(mock_connection)

    assert result is False
    mock_cursor.execute.assert_called_once_with('SELECT TOP(1) 1 FROM Directories')


def test_is_directories_empty_raises_runtime_error_on_exception():
    """Validates the following condition for is_directories_empty:
    1. A failing cursor raises a RuntimeError.

    Args:
        mock_connection (MagicMock): Mock db connection where cursor raises an exception.
    """
    mock_connection = MagicMock()
    mock_connection.cursor.side_effect = Exception('db error')

    with pytest.raises(RuntimeError):
        is_directories_empty(mock_connection)


def test_populate_directories_table_executes_and_commits():
    """Validates the following conditions for populate_directories_table:
    1. executemany is called once with the correct sql and batch list.
    2. commit is called once after the insert.

    Args:
        mock_connection (MagicMock): Mock db connection with cursor returning no rows.
    """
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    batch_list = [(0, 1, 'folder_a'), (1, 2, 'folder_b')]

    populate_directories_table(mock_connection, batch_list)

    mock_cursor.executemany.assert_called_once_with(
        'INSERT INTO Directories (Parent, ProjectID, Name) VALUES (?, ?, ?)', batch_list
    )
    mock_connection.commit.assert_called_once()


def test_populate_directories_table_raises_runtime_error_on_exception():
    """Validates the following conditions for populate_directories_table:
    1. A failing cursor raises a RuntimeError.

    Args:
        mock_connection (MagicMock): Mock db connection where cursor raises an exception.
    """
    mock_connection = MagicMock()
    mock_connection.cursor.side_effect = Exception('db error')

    with pytest.raises(RuntimeError):
        populate_directories_table(mock_connection, [])


def test_is_import_files_empty_returns_true_when_empty():
    """Validates the following conditions for is_import_files_empty:
    1. Returns True when fetchone() returns None (table is empty).

    Args:
        mock_connection (MagicMock): Mock db connection with cursor returning no rows.
    """
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchone.return_value = None

    result = is_import_files_empty(mock_connection)

    assert result is True
    mock_cursor.execute.assert_called_once_with('SELECT TOP(1) 1 FROM ImportFiles')


def test_is_import_files_empty_returns_false_when_not_empty():
    """Validates the following conditions for is_import_files_empty:
    1. Returns False when fetchone() returns a row (table is not empty).

    Args:
        mock_connection (MagicMock): Mock db connection with cursor returning a row.
    """
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchone.return_value = (1,)

    result = is_import_files_empty(mock_connection)

    assert result is False
    mock_cursor.execute.assert_called_once_with('SELECT TOP(1) 1 FROM ImportFiles')


def test_is_import_files_empty_raises_runtime_error_on_exception():
    """Validates the following conditions for is_import_files_empty:
    1. A failing cursor raises a RuntimeError.

    Args:
        mock_connection (MagicMock): Mock db connection where cursor raises an exception.
    """
    mock_connection = MagicMock()
    mock_connection.cursor.side_effect = Exception('db error')

    with pytest.raises(RuntimeError):
        is_import_files_empty(mock_connection)


def test_populate_import_files_table_executes_and_commits():
    """Validates the following conditions for populate_import_files_table:
    1. executemany is called once with the correct sql and batch list.
    2. commit is called once after the insert.

    Args:
        mock_connection (MagicMock): Mock db connection with cursor returning no rows.
    """
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    batch_list = [
        (
            'file_a.txt',
            1,
            1,
            '2026-01-01T00:00:00.000',
            'C:\\folder',
            '2026-01-01T00:00:00.000',
            'abc123',
            1024,
        )
    ]

    populate_import_files_table(mock_connection, batch_list)

    mock_cursor.executemany.assert_called_once_with(
        'INSERT INTO ImportFiles (FileName, DocumentID, ProjectID, ModifyDate, FolderPath, CreateDate, MD5, FileSize) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        batch_list,
    )
    mock_connection.commit.assert_called_once()


def test_populate_import_files_table_raises_runtime_error_on_exception():
    """Validates the following conditions for populate_import_files_table:
    1. A failing cursor raises a RuntimeError.

    Args:
        mock_connection (MagicMock): Mock db connection where cursor raises an exception.
    """
    mock_connection = MagicMock()
    mock_connection.cursor.side_effect = Exception('db error')

    with pytest.raises(RuntimeError):
        populate_import_files_table(mock_connection, [])


def test_clear_files_and_folders_tables_executes_and_commits():
    """Validates the following conditions for clear_files_and_folders_tables:
    1. execute is called for TRUNCATE TABLE dbo.ImportFiles.
    2. execute is called for TRUNCATE TABLE dbo.Directories.
    3. commit is called once after both truncations.

    Args:
        mock_connection (MagicMock): Mock db connection with cursor executing truncations.
    """
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    clear_files_and_folders_tables(mock_connection)

    mock_cursor.execute.assert_any_call('TRUNCATE TABLE dbo.ImportFiles;')
    mock_cursor.execute.assert_any_call('TRUNCATE TABLE dbo.Directories;')
    mock_connection.commit.assert_called_once()


def test_clear_files_and_folders_tables_raises_runtime_error_on_exception():
    """Validates the following conditions for clear_files_and_folders_tables:
    1. A failing cursor raises a RuntimeError.

    Args:
        mock_connection (MagicMock): Mock db connection where cursor raises an exception.
    """
    mock_connection = MagicMock()
    mock_connection.cursor.side_effect = Exception('db error')

    with pytest.raises(RuntimeError):
        clear_files_and_folders_tables(mock_connection)


def test_log_activity_executes_and_commits_with_timestamp():
    """Validates the following conditions for log_activity:
    1. execute is called once with the correct sql, description, and timestamp.
    2. commit is called once after the execute.

    Args:
        mock_connection (MagicMock): Mock db connection with cursor executing the stored procedure.
    """
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    activity_time = datetime(2026, 1, 1, 0, 0, 0)

    log_activity(mock_connection, 'Start populating dbo.Directories', activity_time)

    mock_cursor.execute.assert_called_once_with(
        'EXEC dbo.Activity ?, ?', ('Start populating dbo.Directories', activity_time)
    )
    mock_connection.commit.assert_called_once()


def test_log_activity_executes_and_commits_without_timestamp():
    """Validates the following conditions for log_activity:
    1. execute is called once with the correct sql, description, and None for timestamp.
    2. commit is called once after the execute.

    Args:
        mock_connection (MagicMock): Mock db connection with cursor executing the stored procedure.
    """
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    log_activity(mock_connection, 'Start populating dbo.Directories')

    mock_cursor.execute.assert_called_once_with(
        'EXEC dbo.Activity ?, ?', ('Start populating dbo.Directories', None)
    )
    mock_connection.commit.assert_called_once()


def test_log_activity_raises_runtime_error_on_exception():
    """Validates the following conditions for log_activity:
    1. A failing cursor raises a RuntimeError.

    Args:
        mock_connection (MagicMock): Mock db connection where cursor raises an exception.
    """
    mock_connection = MagicMock()
    mock_connection.cursor.side_effect = Exception('db error')

    with pytest.raises(RuntimeError):
        log_activity(mock_connection, 'Start populating dbo.Directories')
