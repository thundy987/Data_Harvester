from pathlib import Path

import pytest

from sources.windows_fs.WindowsFS import WindowsFS

# ─── __init__ ─────────────────────────────────────────────────────────────────


def test_init_sets_attributes(tmp_path):
    """Validates the following conditions for WindowsFS.__init__:
    1. _source_location is set to the provided path.
    2. file_list contains only files.
    3. folder_list contains only directories.

    Args:
        tmp_path (Path): Pytest fixture providing a temporary directory.
    """
    sub_dir = tmp_path / 'sub'
    sub_dir.mkdir()
    (tmp_path / 'file_a.txt').write_text('hello')

    fs = WindowsFS(str(tmp_path))

    assert fs._source_location == str(tmp_path)
    assert all(f.is_file() for f in fs.file_list)
    assert all(d.is_dir() for d in fs.folder_list)


def test_init_raises_file_not_found_for_invalid_path():
    """Validates the following conditions for WindowsFS.__init__:
    1. Raises FileNotFoundError when the provided path does not exist.

    """
    with pytest.raises(FileNotFoundError):
        WindowsFS('C:\\this\\path\\does\\not\\exist')


# ─── fetch_data ───────────────────────────────────────────────────────────────


def test_fetch_data_folders_sorted_by_depth(tmp_path):
    """Validates the following conditions for WindowsFS.fetch_data:
    1. Folders are sorted shallow to deep by number of path parts.

    Args:
        tmp_path (Path): Pytest fixture providing a temporary directory.
    """
    deep = tmp_path / 'a' / 'b' / 'c'
    deep.mkdir(parents=True)
    shallow = tmp_path / 'x'
    shallow.mkdir()

    fs = WindowsFS(str(tmp_path))
    result = fs.fetch_data()

    depths = [len(Path(f['Path']).parts) for f in result['folders']]
    assert depths == sorted(depths)


def test_fetch_data_returns_correct_folder_records(tmp_path):
    """Validates the following conditions for WindowsFS.fetch_data:
    1. Each folder record contains the correct Path and Name values.

    Args:
        tmp_path (Path): Pytest fixture providing a temporary directory.
    """
    sub_dir = tmp_path / 'folder_a'
    sub_dir.mkdir()

    fs = WindowsFS(str(tmp_path))
    result = fs.fetch_data()

    assert result['folders'][0]['Path'] == sub_dir
    assert result['folders'][0]['Name'] == 'folder_a'


def test_fetch_data_returns_correct_file_records(tmp_path):
    """Validates the following conditions for WindowsFS.fetch_data:
    1. Each file record contains the expected keys with correct values.

    Args:
        tmp_path (Path): Pytest fixture providing a temporary directory.
    """
    test_file = tmp_path / 'file_a.txt'
    test_file.write_text('hello')

    fs = WindowsFS(str(tmp_path))
    result = fs.fetch_data()

    assert len(result['files']) == 1
    file_record = result['files'][0]
    assert file_record['FileName'] == 'file_a.txt'
    assert file_record['FolderPath'] == str(tmp_path)
    assert file_record['FileSize'] == test_file.stat().st_size
    assert file_record['MD5'] is not None
    assert 'CreateDate' in file_record
    assert 'ModifyDate' in file_record


# ─── _extract_properties ──────────────────────────────────────────────────────


def test_extract_properties_returns_correct_dict(tmp_path):
    """Validates the following conditions for WindowsFS._extract_properties:
    1. Returns a dict with all expected keys and correct values for a valid file.

    Args:
        tmp_path (Path): Pytest fixture providing a temporary directory.
    """
    test_file = tmp_path / 'file_a.txt'
    test_file.write_text('hello')

    fs = WindowsFS(str(tmp_path))
    result = fs._extract_properties(test_file)

    assert result['FileName'] == 'file_a.txt'
    assert result['FolderPath'] == str(tmp_path)
    assert result['FileSize'] == test_file.stat().st_size
    assert result['MD5'] is not None
    assert 'CreateDate' in result
    assert 'ModifyDate' in result


def test_extract_properties_raises_file_not_found(tmp_path):
    """Validates the following conditions for WindowsFS._extract_properties:
    1. Raises FileNotFoundError when the provided path does not exist.

    Args:
        tmp_path (Path): Pytest fixture providing a temporary directory.
    """
    fs = WindowsFS(str(tmp_path))

    with pytest.raises(FileNotFoundError):
        fs._extract_properties(tmp_path / 'does_not_exist.txt')
