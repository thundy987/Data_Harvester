from pathlib import Path


def walk_windows_fs(scan_root_directory: str) -> tuple:
    """
    Discovers all subfolders and files contained within a given root directory.
    Args:
        scan_root_directory (str): the root directory to be scanned.
    Raises:
        Exception: 'User supplied an invalid root path'
        Exception: 'Error occurred during file scan'

    Returns:
        file_list (str):
        folder_list (str):
    """

    if not Path(scan_root_directory).exists():
        raise Exception('User supplied an invalid root path')
    try:
        collection = list(Path(scan_root_directory).rglob('*'))

        file_list = [f for f in collection if f.is_file()]

        folder_list = [f for f in collection if f.is_dir()]

        return file_list, folder_list
    except Exception as e:
        raise Exception('Error occurred during file scan') from e
