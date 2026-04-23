from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from sources.PDMDatabase.PDMDatabase import PDMDatabase

# ─── __init__ ─────────────────────────────────────────────────────────────────


def test_pdm_database_init_sets_attributes():
    """Validates the following conditions for PDMDatabase.__init__:
    1. All credentials are stored as instance attributes.
    2. source_location is built from server_name and db_name.

    """
    pdm = PDMDatabase('server\\instance', 'mydb', 'user', 'pass')

    assert pdm.server_name == 'server\\instance'
    assert pdm.db_name == 'mydb'
    assert pdm.username == 'user'
    assert pdm.password == 'pass'
    assert pdm._source_location == 'server\\instance\\mydb'


# ─── fetch_data ───────────────────────────────────────────────────────────────


@patch('sources.PDMDatabase.PDMDatabase.connect_to_db')
def test_fetch_data_raises_runtime_error_when_connect_fails(mock_connect_to_db):
    """Validates the following conditions for PDMDatabase.fetch_data:
    1. Raises RuntimeError when connect_to_db raises.

    Args:
        mock_connect_to_db (MagicMock): Mock replacing connect_to_db.
    """
    mock_connect_to_db.side_effect = ConnectionError('connection failed')

    pdm = PDMDatabase('server\\instance', 'mydb', 'user', 'pass')

    with pytest.raises(RuntimeError):
        pdm.fetch_data()


@patch('sources.PDMDatabase.PDMDatabase.connect_to_db')
def test_fetch_data_closes_connection_on_exception(mock_connect_to_db):
    """Validates the following conditions for PDMDatabase.fetch_data:
    1. db_connection.close() is called in the finally block even when an exception occurs.

    Args:
        mock_connect_to_db (MagicMock): Mock replacing connect_to_db.
    """
    mock_connection = MagicMock()
    mock_connect_to_db.return_value = mock_connection
    mock_connection.cursor.side_effect = Exception('cursor failed')

    pdm = PDMDatabase('server\\instance', 'mydb', 'user', 'pass')

    with pytest.raises(RuntimeError):
        pdm.fetch_data()

    mock_connection.close.assert_called_once()


@patch('sources.PDMDatabase.PDMDatabase.connect_to_db')
def test_fetch_data_folders_normal_path(mock_connect_to_db):
    """Validates the following conditions for PDMDatabase.fetch_data:
    1. A folder row with a non-root FolderPath is appended with trailing backslash removed.
    2. The returned dict contains the correct Name and Path values.

    Args:
        mock_connect_to_db (MagicMock): Mock replacing connect_to_db.
    """
    mock_connection = MagicMock()
    mock_connect_to_db.return_value = mock_connection
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    mock_folder_row = MagicMock()
    mock_folder_row.FolderPath = '\\Projects\\folder_a\\'
    mock_folder_row.FolderName = 'folder_a'

    mock_file_row = MagicMock()
    mock_file_row.FolderPath = '\\Projects\\folder_a\\'
    mock_file_row.FolderName = 'folder_a'
    mock_file_row.FileName = 'file_a.txt'
    mock_file_row.ModifyDate = datetime(2026, 1, 1)
    mock_file_row.CreateDate = datetime(2026, 1, 1)
    mock_file_row.FileSize = 1024

    mock_cursor.__iter__.side_effect = [
        iter([mock_folder_row]),
        iter([mock_file_row]),
    ]

    pdm = PDMDatabase('server\\instance', 'mydb', 'user', 'pass')
    result = pdm.fetch_data()

    assert result['folders'][0]['Name'] == 'folder_a'
    assert result['folders'][0]['Path'] == Path('\\Projects\\folder_a')


@patch('sources.PDMDatabase.PDMDatabase.connect_to_db')
def test_fetch_data_folders_root_path(mock_connect_to_db):
    """Validates the following conditions for PDMDatabase.fetch_data:
    1. A folder row with FolderPath == '\\' uses FolderName as the Path.
    2. The returned dict contains the correct Name and Path values.

    Args:
        mock_connect_to_db (MagicMock): Mock replacing connect_to_db.
    """
    mock_connection = MagicMock()
    mock_connect_to_db.return_value = mock_connection
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    mock_folder_row = MagicMock()
    mock_folder_row.FolderPath = '\\'
    mock_folder_row.FolderName = 'root'

    mock_file_row = MagicMock()
    mock_file_row.FolderPath = '\\Projects\\folder_a\\'
    mock_file_row.FolderName = 'folder_a'
    mock_file_row.FileName = 'file_a.txt'
    mock_file_row.ModifyDate = datetime(2026, 1, 1)
    mock_file_row.CreateDate = datetime(2026, 1, 1)
    mock_file_row.FileSize = 1024

    mock_cursor.__iter__.side_effect = [
        iter([mock_folder_row]),
        iter([mock_file_row]),
    ]

    pdm = PDMDatabase('server\\instance', 'mydb', 'user', 'pass')
    result = pdm.fetch_data()

    assert result['folders'][0]['Name'] == 'root'
    assert result['folders'][0]['Path'] == Path('root')


@patch('sources.PDMDatabase.PDMDatabase.connect_to_db')
def test_fetch_data_skips_bad_folder_row(mock_connect_to_db):
    """Validates the following conditions for PDMDatabase.fetch_data:
    1. A folder row that raises an exception is skipped.
    2. Valid folder rows are still processed.

    Args:
        mock_connect_to_db (MagicMock): Mock replacing connect_to_db.
    """
    mock_connection = MagicMock()
    mock_connect_to_db.return_value = mock_connection
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    bad_folder_row = MagicMock()
    bad_folder_row.FolderPath = (
        None  # None.removesuffix raises AttributeError, caught by except block
    )

    good_folder_row = MagicMock()
    good_folder_row.FolderPath = '\\Projects\\folder_a\\'
    good_folder_row.FolderName = 'folder_a'

    mock_file_row = MagicMock()
    mock_file_row.FolderPath = '\\Projects\\folder_a\\'
    mock_file_row.FolderName = 'folder_a'
    mock_file_row.FileName = 'file_a.txt'
    mock_file_row.ModifyDate = datetime(2026, 1, 1)
    mock_file_row.CreateDate = datetime(2026, 1, 1)
    mock_file_row.FileSize = 1024

    mock_cursor.__iter__.side_effect = [
        iter([bad_folder_row, good_folder_row]),
        iter([mock_file_row]),
    ]

    pdm = PDMDatabase('server\\instance', 'mydb', 'user', 'pass')
    result = pdm.fetch_data()

    assert len(result['folders']) == 1
    assert result['folders'][0]['Name'] == 'folder_a'


@patch('sources.PDMDatabase.PDMDatabase.connect_to_db')
def test_fetch_data_files_normal_path(mock_connect_to_db):
    """Validates the following conditions for PDMDatabase.fetch_data:
    1. A file row with a non-root FolderPath is appended with trailing backslash removed.
    2. The returned dict contains the correct FileName, FolderPath, and MD5 values.

    Args:
        mock_connect_to_db (MagicMock): Mock replacing connect_to_db.
    """
    mock_connection = MagicMock()
    mock_connect_to_db.return_value = mock_connection
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    mock_folder_row = MagicMock()
    mock_folder_row.FolderPath = '\\Projects\\folder_a\\'
    mock_folder_row.FolderName = 'folder_a'

    mock_file_row = MagicMock()
    mock_file_row.FolderPath = '\\Projects\\folder_a\\'
    mock_file_row.FolderName = 'folder_a'
    mock_file_row.FileName = 'file_a.txt'
    mock_file_row.ModifyDate = datetime(2026, 1, 1)
    mock_file_row.CreateDate = datetime(2026, 1, 1)
    mock_file_row.FileSize = 1024

    mock_cursor.__iter__.side_effect = [
        iter([mock_folder_row]),
        iter([mock_file_row]),
    ]

    pdm = PDMDatabase('server\\instance', 'mydb', 'user', 'pass')
    result = pdm.fetch_data()

    assert result['files'][0]['FileName'] == 'file_a.txt'
    assert result['files'][0]['FolderPath'] == '\\Projects\\folder_a'
    assert result['files'][0]['MD5'] is None
    assert result['files'][0]['FileSize'] == 1024


@patch('sources.PDMDatabase.PDMDatabase.connect_to_db')
def test_fetch_data_files_root_path(mock_connect_to_db):
    """Validates the following conditions for PDMDatabase.fetch_data:
    1. A file row with FolderPath == '\\' uses FolderName as the FolderPath.
    2. The returned dict contains the correct FileName and FolderPath values.

    Args:
        mock_connect_to_db (MagicMock): Mock replacing connect_to_db.
    """
    mock_connection = MagicMock()
    mock_connect_to_db.return_value = mock_connection
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    mock_folder_row = MagicMock()
    mock_folder_row.FolderPath = '\\'
    mock_folder_row.FolderName = 'root'

    mock_file_row = MagicMock()
    mock_file_row.FolderPath = '\\'
    mock_file_row.FolderName = 'root'
    mock_file_row.FileName = 'file_a.txt'
    mock_file_row.ModifyDate = datetime(2026, 1, 1)
    mock_file_row.CreateDate = datetime(2026, 1, 1)
    mock_file_row.FileSize = 1024

    mock_cursor.__iter__.side_effect = [
        iter([mock_folder_row]),
        iter([mock_file_row]),
    ]

    pdm = PDMDatabase('server\\instance', 'mydb', 'user', 'pass')
    result = pdm.fetch_data()

    assert result['files'][0]['FileName'] == 'file_a.txt'
    assert result['files'][0]['FolderPath'] == 'root'


@patch('sources.PDMDatabase.PDMDatabase.connect_to_db')
def test_fetch_data_skips_bad_file_row(mock_connect_to_db):
    """Validates the following conditions for PDMDatabase.fetch_data:
    1. A file row that raises an exception is skipped.
    2. Valid file rows are still processed.

    Args:
        mock_connect_to_db (MagicMock): Mock replacing connect_to_db.
    """
    mock_connection = MagicMock()
    mock_connect_to_db.return_value = mock_connection
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    mock_folder_row = MagicMock()
    mock_folder_row.FolderPath = '\\Projects\\folder_a\\'
    mock_folder_row.FolderName = 'folder_a'

    bad_file_row = MagicMock()
    bad_file_row.FolderPath = '\\Projects\\folder_a\\'
    bad_file_row.FileName = 'bad_file.txt'
    bad_file_row.ModifyDate.strftime.side_effect = Exception('bad row')

    good_file_row = MagicMock()
    good_file_row.FolderPath = '\\Projects\\folder_a\\'
    good_file_row.FolderName = 'folder_a'
    good_file_row.FileName = 'file_a.txt'
    good_file_row.ModifyDate = datetime(2026, 1, 1)
    good_file_row.CreateDate = datetime(2026, 1, 1)
    good_file_row.FileSize = 1024

    mock_cursor.__iter__.side_effect = [
        iter([mock_folder_row]),
        iter([bad_file_row, good_file_row]),
    ]

    pdm = PDMDatabase('server\\instance', 'mydb', 'user', 'pass')
    result = pdm.fetch_data()

    assert len(result['files']) == 1
    assert result['files'][0]['FileName'] == 'file_a.txt'
