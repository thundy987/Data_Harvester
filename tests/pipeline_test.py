from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipeline.pipeline import run_directories_pipeline, run_files_pipeline, run_pipeline

# ─── run_directories_pipeline ────────────────────────────────────────────────


@patch('pipeline.pipeline.is_directories_empty')
def test_run_directories_pipeline_raises_when_table_not_empty(
    mock_is_directories_empty,
):
    """Validates the following condition for run_directories_pipeline:
    1. Raises RuntimeError immediately when is_directories_empty returns False.

    Args:
        mock_is_directories_empty (MagicMock): Mock replacing is_directories_empty.
    """
    mock_is_directories_empty.return_value = False
    mock_connection = MagicMock()

    with pytest.raises(RuntimeError):
        run_directories_pipeline(mock_connection, 'C:\\source', [], 10)


@patch('pipeline.pipeline.populate_directories_table')
@patch('pipeline.pipeline.create_project_id')
@patch('pipeline.pipeline.is_directories_empty')
def test_run_directories_pipeline_single_batch(
    mock_is_directories_empty, mock_create_project_id, mock_populate_directories_table
):
    """Validates the following condition for run_directories_pipeline:
    1. populate_directories_table is called once when records fit within a single batch.

    Args:
        mock_is_directories_empty (MagicMock): Mock replacing is_directories_empty.
        mock_create_project_id (MagicMock): Mock replacing create_project_id.
        mock_populate_directories_table (MagicMock): Mock replacing populate_directories_table.
    """
    mock_is_directories_empty.return_value = True
    mock_create_project_id.side_effect = [1, 2, 3]
    mock_connection = MagicMock()

    source_location = 'C:\\source'
    directory_records = [
        {'Path': Path('C:\\source\\folder_a'), 'Name': 'folder_a'},
        {'Path': Path('C:\\source\\folder_b'), 'Name': 'folder_b'},
    ]

    run_directories_pipeline(
        mock_connection, source_location, directory_records, batch_size=10
    )

    mock_populate_directories_table.assert_called_once()


@patch('pipeline.pipeline.populate_directories_table')
@patch('pipeline.pipeline.create_project_id')
@patch('pipeline.pipeline.is_directories_empty')
def test_run_directories_pipeline_exact_batch(
    mock_is_directories_empty, mock_create_project_id, mock_populate_directories_table
):
    """Validates the following condition for run_directories_pipeline:
    1. populate_directories_table is called once mid-loop when records exactly fill the batch size.

    Args:
        mock_is_directories_empty (MagicMock): Mock replacing is_directories_empty.
        mock_create_project_id (MagicMock): Mock replacing create_project_id.
        mock_populate_directories_table (MagicMock): Mock replacing populate_directories_table.
    """
    mock_is_directories_empty.return_value = True
    mock_create_project_id.side_effect = [1, 2, 3]
    mock_connection = MagicMock()

    source_location = 'C:\\source'
    directory_records = [
        {'Path': Path('C:\\source\\folder_a'), 'Name': 'folder_a'},
        {'Path': Path('C:\\source\\folder_b'), 'Name': 'folder_b'},
    ]

    # batch_size=3 matches root + 2 folders exactly
    run_directories_pipeline(
        mock_connection, source_location, directory_records, batch_size=3
    )

    mock_populate_directories_table.assert_called_once()


@patch('pipeline.pipeline.populate_directories_table')
@patch('pipeline.pipeline.create_project_id')
@patch('pipeline.pipeline.is_directories_empty')
def test_run_directories_pipeline_multiple_batches(
    mock_is_directories_empty, mock_create_project_id, mock_populate_directories_table
):
    """Validates the following condition for run_directories_pipeline:
    1. populate_directories_table is called twice when records exceed the batch size.

    Args:
        mock_is_directories_empty (MagicMock): Mock replacing is_directories_empty.
        mock_create_project_id (MagicMock): Mock replacing create_project_id.
        mock_populate_directories_table (MagicMock): Mock replacing populate_directories_table.
    """
    mock_is_directories_empty.return_value = True
    mock_create_project_id.side_effect = [1, 2, 3, 4]
    mock_connection = MagicMock()

    source_location = 'C:\\source'
    directory_records = [
        {'Path': Path('C:\\source\\folder_a'), 'Name': 'folder_a'},
        {'Path': Path('C:\\source\\folder_b'), 'Name': 'folder_b'},
        {'Path': Path('C:\\source\\folder_c'), 'Name': 'folder_c'},
    ]

    # batch_size=2 forces a flush mid-loop and a remainder flush at the end
    run_directories_pipeline(
        mock_connection, source_location, directory_records, batch_size=2
    )

    assert mock_populate_directories_table.call_count == 2


@patch('pipeline.pipeline.populate_directories_table')
@patch('pipeline.pipeline.create_project_id')
@patch('pipeline.pipeline.is_directories_empty')
def test_run_directories_pipeline_returns_correct_lookup_dict(
    mock_is_directories_empty, mock_create_project_id, mock_populate_directories_table
):
    """Validates the following condition for run_directories_pipeline:
    1. Returns a dict mapping folder paths to project IDs.
    2. The dict contains the correct path and project ID entries.

    Args:
        mock_is_directories_empty (MagicMock): Mock replacing is_directories_empty.
        mock_create_project_id (MagicMock): Mock replacing create_project_id.
        mock_populate_directories_table (MagicMock): Mock replacing populate_directories_table.
    """
    mock_is_directories_empty.return_value = True
    mock_create_project_id.side_effect = [1, 2]
    mock_connection = MagicMock()

    source_location = 'C:\\source'
    directory_records = [
        {'Path': Path('C:\\source\\folder_a'), 'Name': 'folder_a'},
    ]

    result = run_directories_pipeline(
        mock_connection, source_location, directory_records, batch_size=10
    )

    assert isinstance(result, dict)
    assert result['C:\\source'] == 1
    assert result['C:\\source\\folder_a'] == 2


@patch('pipeline.pipeline.populate_directories_table')
@patch('pipeline.pipeline.create_project_id')
@patch('pipeline.pipeline.is_directories_empty')
def test_run_directories_pipeline_skips_bad_records(
    mock_is_directories_empty, mock_create_project_id, mock_populate_directories_table
):
    """Validates the following condition for run_directories_pipeline:
    1. A record missing required keys is skipped.
    2. Valid records are still processed.

    Args:
        mock_is_directories_empty (MagicMock): Mock replacing is_directories_empty.
        mock_create_project_id (MagicMock): Mock replacing create_project_id.
        mock_populate_directories_table (MagicMock): Mock replacing populate_directories_table.
    """
    mock_is_directories_empty.return_value = True
    mock_create_project_id.side_effect = [1, 2]
    mock_connection = MagicMock()

    source_location = 'C:\\source'
    directory_records = [
        {'Path': None, 'Name': 'bad_folder'},  # will raise when accessing .parent
        {'Path': Path('C:\\source\\folder_a'), 'Name': 'folder_a'},
    ]

    result = run_directories_pipeline(
        mock_connection, source_location, directory_records, batch_size=10
    )

    assert 'C:\\source\\folder_a' in result


# ─── run_files_pipeline ───────────────────────────────────────────────────────


@patch('pipeline.pipeline.populate_import_files_table')
@patch('pipeline.pipeline.create_document_id')
@patch('pipeline.pipeline.is_import_files_empty')
def test_run_files_pipeline_raises_when_table_not_empty(
    mock_is_import_files_empty,
    mock_create_document_id,
    mock_populate_import_files_table,
):
    """Validates the following condition for run_files_pipeline:
    1. Raises RuntimeError immediately when is_import_files_empty returns False.

    Args:
        mock_is_import_files_empty (MagicMock): Mock replacing is_import_files_empty.
        mock_create_document_id (MagicMock): Mock replacing create_document_id.
        mock_populate_import_files_table (MagicMock): Mock replacing populate_import_files_table.
    """
    mock_is_import_files_empty.return_value = False
    mock_connection = MagicMock()

    with pytest.raises(RuntimeError):
        run_files_pipeline(mock_connection, [], {}, 10)


@patch('pipeline.pipeline.populate_import_files_table')
@patch('pipeline.pipeline.create_document_id')
@patch('pipeline.pipeline.is_import_files_empty')
def test_run_files_pipeline_single_batch(
    mock_is_import_files_empty,
    mock_create_document_id,
    mock_populate_import_files_table,
):
    """Validates the following condition for run_files_pipeline:
    1. populate_import_files_table is called once when records fit within a single batch.

    Args:
        mock_is_import_files_empty (MagicMock): Mock replacing is_import_files_empty.
        mock_create_document_id (MagicMock): Mock replacing create_document_id.
        mock_populate_import_files_table (MagicMock): Mock replacing populate_import_files_table.
    """
    mock_is_import_files_empty.return_value = True
    mock_create_document_id.side_effect = [1, 2]
    mock_connection = MagicMock()

    directory_lookup = {'C:\\source\\folder_a': 1}
    file_records = [
        {
            'FileName': 'file_a.txt',
            'FolderPath': 'C:\\source\\folder_a',
            'ModifyDate': '2026-01-01T00:00:00.000',
            'CreateDate': '2026-01-01T00:00:00.000',
            'MD5': 'abc123',
            'FileSize': 1024,
        },
        {
            'FileName': 'file_b.txt',
            'FolderPath': 'C:\\source\\folder_a',
            'ModifyDate': '2026-01-01T00:00:00.000',
            'CreateDate': '2026-01-01T00:00:00.000',
            'MD5': 'def456',
            'FileSize': 2048,
        },
    ]

    run_files_pipeline(mock_connection, file_records, directory_lookup, batch_size=10)

    mock_populate_import_files_table.assert_called_once()


@patch('pipeline.pipeline.populate_import_files_table')
@patch('pipeline.pipeline.create_document_id')
@patch('pipeline.pipeline.is_import_files_empty')
def test_run_files_pipeline_multiple_batches(
    mock_is_import_files_empty,
    mock_create_document_id,
    mock_populate_import_files_table,
):
    """Validates the following condition for run_files_pipeline:
    1. populate_import_files_table is called twice when records exceed the batch size.

    Args:
        mock_is_import_files_empty (MagicMock): Mock replacing is_import_files_empty.
        mock_create_document_id (MagicMock): Mock replacing create_document_id.
        mock_populate_import_files_table (MagicMock): Mock replacing populate_import_files_table.
    """
    mock_is_import_files_empty.return_value = True
    mock_create_document_id.side_effect = [1, 2, 3]
    mock_connection = MagicMock()

    directory_lookup = {'C:\\source\\folder_a': 1}
    file_records = [
        {
            'FileName': 'file_a.txt',
            'FolderPath': 'C:\\source\\folder_a',
            'ModifyDate': '2026-01-01T00:00:00.000',
            'CreateDate': '2026-01-01T00:00:00.000',
            'MD5': 'abc123',
            'FileSize': 1024,
        },
        {
            'FileName': 'file_b.txt',
            'FolderPath': 'C:\\source\\folder_a',
            'ModifyDate': '2026-01-01T00:00:00.000',
            'CreateDate': '2026-01-01T00:00:00.000',
            'MD5': 'def456',
            'FileSize': 2048,
        },
        {
            'FileName': 'file_c.txt',
            'FolderPath': 'C:\\source\\folder_a',
            'ModifyDate': '2026-01-01T00:00:00.000',
            'CreateDate': '2026-01-01T00:00:00.000',
            'MD5': 'ghi789',
            'FileSize': 4096,
        },
    ]

    # batch_size=2 forces a flush mid-loop and a remainder flush at the end
    run_files_pipeline(mock_connection, file_records, directory_lookup, batch_size=2)

    assert mock_populate_import_files_table.call_count == 2


@patch('pipeline.pipeline.populate_import_files_table')
@patch('pipeline.pipeline.create_document_id')
@patch('pipeline.pipeline.is_import_files_empty')
def test_run_files_pipeline_skips_bad_records(
    mock_is_import_files_empty,
    mock_create_document_id,
    mock_populate_import_files_table,
):
    """Validates the following condition for run_files_pipeline:
    1. A file record with a missing FolderPath key is skipped.
    2. Valid records are still processed.

    Args:
        mock_is_import_files_empty (MagicMock): Mock replacing is_import_files_empty.
        mock_create_document_id (MagicMock): Mock replacing create_document_id.
        mock_populate_import_files_table (MagicMock): Mock replacing populate_import_files_table.
    """
    mock_is_import_files_empty.return_value = True
    mock_create_document_id.side_effect = [1, 2]
    mock_connection = MagicMock()

    directory_lookup = {'C:\\source\\folder_a': 1}
    file_records = [
        {'FileName': 'bad_file.txt'},  # missing required keys, will raise KeyError
        {
            'FileName': 'file_a.txt',
            'FolderPath': 'C:\\source\\folder_a',
            'ModifyDate': '2026-01-01T00:00:00.000',
            'CreateDate': '2026-01-01T00:00:00.000',
            'MD5': 'abc123',
            'FileSize': 1024,
        },
    ]

    run_files_pipeline(mock_connection, file_records, directory_lookup, batch_size=10)

    mock_populate_import_files_table.assert_called_once()


# ─── run_pipeline ─────────────────────────────────────────────────────────────


@patch('pipeline.pipeline.run_directories_pipeline')
@patch('pipeline.pipeline.log_activity')
def test_run_pipeline_raises_when_directories_pipeline_fails(
    mock_log_activity, mock_run_directories_pipeline
):
    """Validates the following condition for run_pipeline:
    1. Raises RuntimeError when run_directories_pipeline raises.

    Args:
        mock_log_activity (MagicMock): Mock replacing log_activity.
        mock_run_directories_pipeline (MagicMock): Mock replacing run_directories_pipeline.
    """
    mock_run_directories_pipeline.side_effect = RuntimeError(
        'directories pipeline failed'
    )
    mock_connection = MagicMock()
    mock_source = MagicMock()
    mock_source.fetch_data.return_value = {'folders': [], 'files': []}

    with pytest.raises(RuntimeError):
        run_pipeline(mock_connection, 10, mock_source)


@patch('pipeline.pipeline.run_files_pipeline')
@patch('pipeline.pipeline.run_directories_pipeline')
@patch('pipeline.pipeline.log_activity')
def test_run_pipeline_raises_when_files_pipeline_fails(
    mock_log_activity, mock_run_directories_pipeline, mock_run_files_pipeline
):
    """Validates the following condition for run_pipeline:
    1. Raises RuntimeError when run_files_pipeline raises.

    Args:
        mock_log_activity (MagicMock): Mock replacing log_activity.
        mock_run_directories_pipeline (MagicMock): Mock replacing run_directories_pipeline.
        mock_run_files_pipeline (MagicMock): Mock replacing run_files_pipeline.
    """
    mock_run_directories_pipeline.return_value = {}
    mock_run_files_pipeline.side_effect = RuntimeError('files pipeline failed')
    mock_connection = MagicMock()
    mock_source = MagicMock()
    mock_source.fetch_data.return_value = {'folders': [], 'files': []}

    with pytest.raises(RuntimeError):
        run_pipeline(mock_connection, 10, mock_source)
