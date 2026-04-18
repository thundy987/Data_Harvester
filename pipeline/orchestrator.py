from pathlib import Path

from db.repository import (
    is_directories_empty,
    is_import_files_empty,
    log_activity,
    populate_directories_table,
    populate_import_files_table,
)
from pipeline.transformer import cleanse_file_record
from sources.base import SourceSystem
from utils.id_generator import create_document_id, create_project_id
from utils.logger import logger


def run_directories_pipeline(
    db_connection, source_location: str, all_folders: list, batch_size: int
) -> dict:
    """Creates a list of tuples that's sent to populate_directories_table when the batch_size is full.

    Args:
        db_connection (Connection): Connection details for the target db.
        source_location (str): The directory being harvested and loaded (needed for root folder(s)).
        all_folders (list): list of all folders in the scan.
        batch_size (int): How many records to load into the batch insert statement.

    Raises:
        Exception: 'Directories table is not empty'

    Returns:
        lookup_dict: A dictionary with {full_path: ProjectID} form.
    """
    if not is_directories_empty(db_connection):
        logger.error('Directories table is not empty')
        raise Exception('Directories table is not empty')

    sorted_folders = sorted(all_folders, key=lambda p: len(p.parts))

    lookup_dict = {}

    # set the root first
    directory_record = {
        'Parent': 0,
        'ProjectID': create_project_id(),
        'Name': Path(source_location).name,
    }

    # store {folder_path: ProjectID} for reference
    lookup_dict[source_location] = directory_record['ProjectID']

    # batch inserting with executemany() requires a list of tuples
    batch_list = [
        (
            directory_record.get('Parent'),
            directory_record.get('ProjectID'),
            directory_record.get('Name'),
        )
    ]

    # iterate subfolders
    for folder in sorted_folders:
        try:
            parent_folder = folder.parent

            directory_record = {
                'Parent': lookup_dict.get(str(parent_folder), 0),
                'ProjectID': create_project_id(),
                'Name': folder.name,
            }

            # convert dict record to tuple and append to list
            batch_list.append(
                (
                    directory_record.get('Parent'),
                    directory_record.get('ProjectID'),
                    directory_record.get('Name'),
                )
            )
            # push and reset batch when it's full
            if len(batch_list) == batch_size:
                populate_directories_table(db_connection, batch_list)
                logger.info(
                    f'Flushing batch of {batch_size} directory records to database'
                )
                batch_list = []

            lookup_dict[str(folder)] = directory_record['ProjectID']

        except Exception as e:
            logger.error(f'Skipping folder {folder}: {e}')
            continue
    # push any remaining records if batch size not reached
    if batch_list:
        populate_directories_table(db_connection, batch_list)
        logger.info(f'Flushing batch of {batch_size} directory records to database')
    return lookup_dict


def run_files_pipeline(
    db_connection,
    all_files: list,
    directory_lookup: dict,
    batch_size: int,
    source: SourceSystem,
) -> None:
    """Populates the ImportFiles table using the cleaned up data and the ProjectID from run_directories_pipeline()

    Args:
        db_connection (Connection): Connection details for the target db.
        all_files (list): The files being harvested and loaded.
        directory_lookup (dict): Directories dictionary to provide ProjectID
        batch_size (int): How many records to load into the batch insert statement.
        source (SourceSystem): The data source object.

    Raises:
        Exception: raise Exception('ImportFiles table is not empty')
    """
    if not is_import_files_empty(db_connection):
        logger.error('ImportFiles table is not empty')
        raise Exception('ImportFiles table is not empty')

    batch_list = []

    for file in all_files:
        try:
            props_dirty = source.extract_properties(file)

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

            # convert dict record to tuple and append to list
            batch_list.append(
                (
                    file_record.get('FileName'),
                    file_record.get('DocumentID'),
                    file_record.get('ProjectID'),
                    file_record.get('ModifyDate'),
                    file_record.get('FolderPath'),
                    file_record.get('CreateDate'),
                    file_record.get('MD5'),
                    file_record.get('FileSize'),
                )
            )

            # push and reset batch when it's full
            if len(batch_list) == batch_size:
                populate_import_files_table(db_connection, batch_list)
                logger.info(f'Flushing batch of {batch_size} file records to database')
                batch_list = []

        except Exception as e:
            logger.error(f'Skipping file {file}: {e}')
            continue
    # push any remaining records if batch size not reached
    if batch_list:
        populate_import_files_table(db_connection, batch_list)
        logger.info(f'Flushing batch of {batch_size} file records to database')


def run_pipeline(db_connection, batch_size: int, source: SourceSystem) -> None:
    """Executes the full loading pipeline into the target db.

    Args:
        db_connection (Connection): Connection details for the target db.
        batch_size (int): How many records to load into the batch insert statement.
        source (SourceSystem): The data source object.

    Raises:
        Exception: 'Directories pipeline failed, no files loaded'
        Exception: 'Files pipeline failed.'
    """
    try:
        all_files, all_folders = source.fetch_data()
        log_activity(db_connection, 'Start populating dbo.Directories')
        logger.info('Start populating dbo.Directories')
        directory_lookup = run_directories_pipeline(
            db_connection, source.source_location, all_folders, batch_size
        )
        log_activity(db_connection, 'Finish populating dbo.Directories')
        logger.info('Finish populating dbo.Directories')
    except Exception as e:
        logger.error(f'Directories pipeline failed, no files loaded: {e}')
        raise Exception('Directories pipeline failed, no files loaded') from e

    try:
        log_activity(db_connection, 'Start populating dbo.ImportFiles')
        logger.info('Start populating dbo.ImportFiles')
        run_files_pipeline(
            db_connection, all_files, directory_lookup, batch_size, source
        )
        log_activity(db_connection, 'Finish populating dbo.ImportFiles')
        logger.info('Finish populating dbo.ImportFiles')

    except Exception as e:
        logger.error(f'Files pipeline failed: {e}')
        raise Exception('Files pipeline failed.') from e
