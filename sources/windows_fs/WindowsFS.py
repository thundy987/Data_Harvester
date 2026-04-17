import hashlib
from pathlib import Path

from sources.base import SourceSystem
from utils.logger import logger


class WindowsFS(SourceSystem):
    def fetch_data(self, scan_root_directory: str) -> tuple[list[Path], list[Path]]:
        """
        Discovers all subfolders and files contained within a given root directory.
        Args:
            scan_root_directory (str): the root directory to be scanned.
        Raises:
            Exception: 'User supplied an invalid root path'
            Exception: 'Error occurred during file scan'

        Returns:
            file_list (list[Path]):
            folder_list (list[Path]):
        """

        if not Path(scan_root_directory).exists():
            logger.error('User supplied an invalid root path')
            raise Exception('User supplied an invalid root path')
        try:
            collection = list(Path(scan_root_directory).rglob('*'))

            file_list = [f for f in collection if f.is_file()]

            folder_list = [f for f in collection if f.is_dir()]

            return file_list, folder_list
        except Exception as e:
            logger.error(f'Error occurred during file scan: {e}')
            raise Exception('Error occurred during file scan') from e

    def extract_properties(self, path_to_file: Path | str) -> tuple:
        """Extracts meta data from a file, given the path of the file.

        Args:
            path_to_file (Path | str): path to the file.

        Raises:
            Exception: 'User supplied an invalid file path'
            Exception: 'Error occurred while extracting file metadata'

        Returns:
            parent_folder (str): the folder in which the file lives.
            file_name (str): the file name including extension (without folder path).
            file_extension (str): the file extension
            create_date (float): Unix format date when file was created
            last_modified_date (float): Unix format date when file was last modified
            file_size (int): file size in bytes
            md5_hash (str): MD5 hash checksum of file.
        """

        if not Path(path_to_file).exists():
            logger.error('User supplied an invalid file path')
            raise Exception('User supplied an invalid file path')
        try:
            # temp variables
            target_file = Path(path_to_file)
            file_stats = target_file.stat()

            # real variables
            parent_folder = target_file.parent
            file_name = target_file.name
            file_extension = target_file.suffix
            create_date = file_stats.st_birthtime
            last_modified_date = file_stats.st_mtime
            file_size = file_stats.st_size

            # MD5 Checksum
            with open(path_to_file, 'rb') as f:
                h = hashlib.file_digest(f, 'md5')
                md5_hash = h.hexdigest()

            return (
                parent_folder,
                file_name,
                file_extension,
                create_date,
                last_modified_date,
                file_size,
                md5_hash,
            )
        except Exception as e:
            logger.error(f'Error occurred while extracting file metadata: {e}')
            raise Exception('Error occurred while extracting file metadata') from e
