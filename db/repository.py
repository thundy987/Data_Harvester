from datetime import datetime

from utils.logger import logger


def is_directories_empty(db_connection) -> bool:
    """Checks if the Directories table is empty. This should be empty before the pipeline writes to it.

    Args:
        db_connection (Connection): Db connection details.

    Raises:
        Exception: 'Error querying Directories table'

    Returns:
        bool: True if the table is empty, False if the table is not empty.
    """
    try:
        with db_connection.cursor() as cursor:
            sql = 'SELECT TOP(1) 1 FROM Directories'

            cursor.execute(sql)

            return False if cursor.fetchone() else True

    except Exception as e:
        logger.error(f'Error querying Directories table: {e}')
        raise Exception('Error querying Directories table') from e


def populate_directories_table(db_connection, batch_list: list) -> None:
    """Writes a batch of records to the Directories table of the target db.

    Args:
        db_connection (Connection): Db connection details.
        batch_list (list): ProjectID, Name, Parent being written to the db.

    Raises:
        Exception: 'Error writing to Directories table'
    """
    try:
        with db_connection.cursor() as cursor:  # 'with' used to auto-close cursor
            cursor.fast_executemany = True

            # insert it batches

            sql = 'INSERT INTO Directories (Parent, ProjectID, Name) VALUES (?, ?, ?)'  # '?' placeholder prevents sql injection.

            cursor.executemany(sql, batch_list)

            # 3. Commit the transaction
            db_connection.commit()

    except Exception as e:
        logger.error(f'Error writing to Directories table {e}')
        raise Exception('Error writing to Directories table') from e


def is_import_files_empty(db_connection) -> bool:
    """Checks if the ImportFiles table is empty. This should be empty before the pipeline writes to it.

    Args:
        db_connection (Connection): Db connection details.

    Raises:
        Exception: 'Error querying ImportFiles table'

    Returns:
        bool: True if the table is empty, False if the table is not empty.
    """
    try:
        with db_connection.cursor() as cursor:
            sql = 'SELECT TOP(1) 1 FROM ImportFiles'

            cursor.execute(sql)

            return False if cursor.fetchone() else True

    except Exception as e:
        logger.error(f'Error querying ImportFiles table {e}')
        raise Exception('Error querying ImportFiles table') from e


def populate_import_files_table(db_connection, batch_list: list) -> None:
    """Writes a batch of records to the ImportFiles table of the target db.

    Args:
        db_connection (Connection): Db connection details.
        batch_list (list): FileName, DocumentID, ProjectID, ModifyDate, FolderPath, CreateDate, MD5, FileSize being written to the db.

    Raises:
        Exception: 'Error writing to ImportFiles table'
    """
    try:
        with db_connection.cursor() as cursor:  # 'with' used to auto-close cursor
            cursor.fast_executemany = True

            sql = 'INSERT INTO ImportFiles (FileName, DocumentID, ProjectID, ModifyDate, FolderPath, CreateDate, MD5, FileSize) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'  # '?' placeholder prevents sql injection.

            cursor.executemany(sql, batch_list)

            # 3. Commit the transaction
            db_connection.commit()

    except Exception as e:
        logger.error(f'Error writing to ImportFiles table {e}')
        raise Exception('Error writing to ImportFiles table') from e


def clear_files_and_folders_tables(db_connection) -> None:
    """Truncates the Import Files and Directories tables in the target db.

    Args:
        db_connection (Connection): Db connection details.

    Raises:
        Exception: 'Error clearing tables'
    """
    try:
        with db_connection.cursor() as cursor:
            sql_one = 'TRUNCATE TABLE dbo.ImportFiles;'

            cursor.execute(sql_one)

            sql_two = 'TRUNCATE TABLE dbo.Directories;'

            cursor.execute(sql_two)

            db_connection.commit()
    except Exception as e:
        logger.error(f'Error clearing tables: {e}')
        raise Exception('Error clearing tables') from e


def log_activity(
    db_connection, activity_description: str, activity_time: datetime | None = None
) -> None:
    """Executes the Activity stored procedure in the target db.

    Args:
        db_connection (Connection): Db connection details.
        activity_description (str): Description of the activity
        activity_time (datetime | None, optional): activity time stamp - Defaults to None.

    Raises:
        Exception: 'Error executing Activity stored procedure'
    """
    try:
        with db_connection.cursor() as cursor:
            sql = 'EXEC dbo.Activity ?, ?'

            cursor.execute(sql, (activity_description, activity_time))

            db_connection.commit()
    except Exception as e:
        logger.error(f'Error executing Activity stored procedure: {e}')
        raise Exception('Error executing Activity stored procedure') from e
