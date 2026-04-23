import hashlib
from pathlib import Path

from pipeline.transformer import (
    find_illegal_characters_in_file_name,
    format_unix_date_to_iso,
    remove_white_spaces,
)
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
            FileNotFoundError: If the root path does not exist.
            RuntimeError: If the file system scan fails.

        """
        # Store value in 'private backing variabe' to avoid recursion calling the base class property.
        self._source_location = source_location

        if not Path(source_location).exists():
            logger.error(f'Root path not found: {source_location}')
            raise FileNotFoundError(f'Root path not found: {source_location}')
        try:
            collection = list(Path(source_location).rglob('*'))

            self.file_list = [f for f in collection if f.is_file()]

            self.folder_list = [f for f in collection if f.is_dir()]

        except OSError as e:
            logger.error(f'File system scan failed at {source_location}: {e}')
            raise RuntimeError(f'File system scan failed at {source_location}') from e

    @property
    def source_location(self) -> str:
        """
        Root directory path provided at instantiation. Returns the private backing variable to satisfy the SourceSystem abstract property contract.
        """
        return self._source_location

    def fetch_data(self) -> dict[str, list[dict]]:
        """Collects folder and file information from the source location.

        Returns:
            dict[str, list[dict]]: A dictionary with 'folders' and 'files' keys, each containing a list of record dicts.
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
                logger.warning(f'Skipping folder {folder}: {e}')
                continue

        ######## File Work ########
        file_records = []

        for file in self.file_list:
            try:
                props_dirty = self._extract_properties(file)

                props_clean = self._cleanse_file_record(props_dirty)
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
                logger.warning(f'Skipping file {file}: {e}')
                continue
        return {'folders': directory_records, 'files': file_records}

    def _extract_properties(self, path_to_file: Path | str) -> dict:
        """Extracts meta data from a file, given the path of the file.

        Args:
            path_to_file (Path | str): path to the file.

        Raises:
            FileNotFoundError: If the file path does not exist.
            OSError: If reading file metadata or computing the checksum fails.

        Returns:
            dict: metadata properties defined as dictionary
        """

        if not Path(path_to_file).exists():
            raise FileNotFoundError(f'File not found: {path_to_file}')
        try:
            # temp variables
            target_file = Path(path_to_file)
            file_stats = target_file.stat()

            # real variables
            parent_folder = target_file.parent
            file_name = target_file.name
            create_date = file_stats.st_birthtime
            last_modified_date = file_stats.st_mtime
            file_size = file_stats.st_size

            # MD5 Checksum
            with open(path_to_file, 'rb') as f:
                h = hashlib.file_digest(f, 'md5')
                md5_hash = h.hexdigest()

            return {
                'FolderPath': str(parent_folder),
                'FileName': file_name,
                'CreateDate': create_date,
                'ModifyDate': last_modified_date,
                'FileSize': file_size,
                'MD5': md5_hash,
            }
        except OSError as e:
            logger.error(f'Failed to extract metadata from {path_to_file}: {e}')
            raise

    def _cleanse_file_record(self, raw_metadeta: dict) -> dict:
        """Executes cleansing functions as needed on the source data to conform to target data model.

        Args:
            raw_metadeta (dict): metadata of a harvested file

        Returns:
            dict: cleansed metadata of the file
        """
        file_name = remove_white_spaces(raw_metadeta['FileName'])

        if file_name:  # make sure file_name is not None
            illegal = find_illegal_characters_in_file_name(file_name)
            if illegal:
                logger.warning(f'{file_name} contains illegal characters: {illegal}')

        create_date = format_unix_date_to_iso(raw_metadeta['CreateDate'])
        modify_date = format_unix_date_to_iso(raw_metadeta['ModifyDate'])

        return {
            'FileName': file_name,
            'ModifyDate': modify_date,
            'FolderPath': raw_metadeta['FolderPath'],
            'CreateDate': create_date,
            'MD5': raw_metadeta['MD5'],
            'FileSize': raw_metadeta['FileSize'],
        }
