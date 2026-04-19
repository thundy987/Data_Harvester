import hashlib
from pathlib import Path

from pipeline.transformer import cleanse_file_record
from sources.base import SourceSystem
from utils.logger import logger


class WindowsFS(SourceSystem):
    def __init__(self, source_location: str):
        """
        Initializes the WindowFS scanner and discovers all subfolders and files contained within a given root directory.
        Args:
            source_location (str): the root directory to be scanned.

        Attributes:
            file_list (list[Path]): All file objects found under the root.
            folder_list (list[Path]): All directory objects found under the root.

        Raises:
            FileNotFoundError: 'User supplied an invalid root path'
            RuntimeError: 'Error occurred during file scan'

        """
        # Store value in 'private backing variabe' to avoid recursion calling the base class property.
        self._source_location = source_location

        if not Path(source_location).exists():
            logger.error('User supplied an invalid root path')
            raise FileNotFoundError(f'Root path not found: {source_location}')
        try:
            collection = list(Path(source_location).rglob('*'))

            self.file_list = [f for f in collection if f.is_file()]

            self.folder_list = [f for f in collection if f.is_dir()]

        except Exception as e:
            logger.error(f'Error occurred during file scan: {e}')
            raise RuntimeError('Error occurred during file scan') from e

    @property
    def source_location(self) -> str:
        """
        Root directory path provided at instantiation. Returns the private backing variable to satisfy the SourceSystem abstract property contract.
        """
        return self._source_location

    def fetch_data(self) -> tuple[list[dict], list[dict]]:
        """Collects folder and file information from the source location.

        Returns:
            tuple[list[dict], list[dict]]: The directories and files with their properties.
        """
        ######## Folder Work ########
        sorted_folders = sorted(self.folder_list, key=lambda p: len(p.parts))

        directory_records = []

        for folder in sorted_folders:
            try:
                directory_records.append(
                    {
                        'Path': folder,
                        'Name': folder.name,
                    }
                )
            except Exception as e:
                logger.error(f'Skipping folder {folder}: {e}')
                continue

        ######## File Work ########
        file_records = []

        for file in self.file_list:
            try:
                props_dirty = self._extract_properties(file)

                props_clean = cleanse_file_record(props_dirty)
                file_records.append(
                    {
                        'FileName': props_clean['FileName'],
                        'ModifyDate': props_clean['ModifyDate'],
                        'FolderPath': props_clean['FolderPath'],
                        'CreateDate': props_clean['CreateDate'],
                        'MD5': props_clean['MD5'],
                        'FileSize': props_clean['FileSize'],
                    }
                )
            except Exception as e:
                logger.error(f'Skipping file {file}: {e}')
                continue
        return (directory_records, file_records)

    def _extract_properties(self, path_to_file: Path | str) -> tuple:
        """Extracts meta data from a file, given the path of the file.

        Args:
            path_to_file (Path | str): path to the file.

        Raises:
            FileNotFoundError: 'User supplied an invalid file path'
            Exception: 'Error occurred while extracting file metadata'

        Returns:
            parent_folder (str): the folder in which the file lives.
            file_name (str): the file name including extension (without folder path).
            create_date (float): Unix format date when file was created
            last_modified_date (float): Unix format date when file was last modified
            file_size (int): file size in bytes
            md5_hash (str): MD5 hash checksum of file.
        """

        if not Path(path_to_file).exists():
            logger.error('User supplied an invalid file path')
            raise FileNotFoundError(f'File path not found: {path_to_file}')
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
            raise RuntimeError('Error occurred while extracting file metadata') from e
