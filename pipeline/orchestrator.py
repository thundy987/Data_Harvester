from pathlib import Path

from db.repository import (
    is_directories_empty,
    is_import_files_empty,
    log_activity,
    populate_directories_table,
    populate_import_files_table,
)
from pipeline.transformer import cleanse_file_record
from sources.windows_fs.metadata import extract_file_properties
from sources.windows_fs.scanner import walk_windows_fs
from utils.id_generator import create_document_id, create_project_id
from utils.logger import logger


# TODO walk_windows_fs() being called twice - call once and pass to both pipelines?
def run_directories_pipeline(db_connection, scan_root_directory: str) -> dict:
    """Creates a dictionary that will be used to populate the Directories table
    with the folder path and a ProjectID.

    Args:
        db_connection (Connection): Connection details for the target db.
        scan_root_directory (str): The directory being harvested and loaded.

    Returns:
        lookup_dict (dict): A dictionary with {full_path: ProjectID} form.
    """
    if not is_directories_empty(db_connection):
        raise Exception('Directories table is not empty')

    _, all_folders = walk_windows_fs(scan_root_directory)

    sorted_folders = sorted(all_folders, key=lambda p: len(p.parts))

    lookup_dict = {}

    # set the root first
    directory_record = {
        'Parent': 0,
        'ProjectID': create_project_id(),
        'Name': Path(scan_root_directory).name,
    }

    populate_directories_table(db_connection, directory_record)
    lookup_dict[scan_root_directory] = directory_record['ProjectID']

    # iterate subfolders
    for folder in sorted_folders:
        try:
            parent_folder = folder.parent

            directory_record = {
                'Parent': lookup_dict.get(str(parent_folder), 0),
                'ProjectID': create_project_id(),
                'Name': folder.name,
            }

            populate_directories_table(db_connection, directory_record)

            lookup_dict[str(folder)] = directory_record['ProjectID']
        except Exception as e:
            logger.error(f'Skipping folder {folder}: {e}')
            continue
    return lookup_dict


def run_files_pipeline(
    db_connection, scan_root_directory: str, directory_lookup: dict
) -> None:
    """Populates the ImportFiles table using the cleaned up data and the ProjectID from run_directories_pipeline()

    Args:
        db_connection (Connection): Connection details for the target db.
        scan_root_directory (str): The directory being harvested and loaded.
        directory_lookup (dict): Directories dictionary to provide ProjectID

    Raises:
        Exception: raise Exception('ImportFiles table is not empty')
    """
    if not is_import_files_empty(db_connection):
        raise Exception('ImportFiles table is not empty')

    all_files, _ = walk_windows_fs(scan_root_directory)

    for file in all_files:
        try:
            props_dirty = extract_file_properties(file)

            props_clean = cleanse_file_record(props_dirty)

            file_record = {
                'FileName': props_clean['FileName'],
                'DocumentID': create_document_id(),
                'ProjectID': directory_lookup[props_clean['FolderPath']],
                'ModifyDate': props_clean['ModifyDate'],
                'FolderPath': props_clean['FolderPath'],
                'CreateDate': props_clean['CreateDate'],
                'MD5': props_clean['MD5'],
                'FileSize': props_clean['FileSize'],
            }

            populate_import_files_table(db_connection, file_record)

        except Exception as e:
            logger.error(f'Skipping file {file}: {e}')
            continue


def run_pipeline(db_connection, scan_root_directory: str) -> None:
    """Executes the full loading pipeline into the target db.

    Args:
        db_connection (Connection): Connection details for the target db.
        scan_root_directory (str): The directory being harvested and loaded.

    Raises:
        Exception: 'Directories pipeline failed, no files loaded'
        Exception: 'Files pipeline failed.'
    """
    try:
        log_activity(db_connection, 'Start populating dbo.Directories')
        logger.info('Start populating dbo.Directories')
        directory_lookup = run_directories_pipeline(db_connection, scan_root_directory)
        log_activity(db_connection, 'Finish populating dbo.Directories')
        logger.info('Finish populating dbo.Directories')
    except Exception as e:
        logger.error(f'Directories pipeline failed, no files loaded: {e}')
        raise Exception('Directories pipeline failed, no files loaded') from e

    try:
        log_activity(db_connection, 'Start populating dbo.ImportFiles')
        logger.info('Start populating dbo.ImportFiles')
        run_files_pipeline(db_connection, scan_root_directory, directory_lookup)
        log_activity(db_connection, 'Finish populating dbo.ImportFiles')
        logger.info('Finish populating dbo.ImportFiles')

    except Exception as e:
        logger.error(f'Files pipeline failed: {e}')
        raise Exception('Files pipeline failed.') from e
