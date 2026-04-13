def populate_directories_table(db_connection, directory_record: dict) -> None:
    """Writes a single record to the Directories table of the target db.

    Args:
        db_connection (Connection): Db connection details.
        directory_record (dict): ProjectID, Name, Parent being written to the db.

    Raises:
        Exception: 'Error connecting to target db'
    """
    try:
        # 1. Establish connection
        cursor = db_connection.cursor()

        # 2. Execute Insert
        sql = 'INSERT INTO Directories (ProjectID, Name, Parent) VALUES (?, ?, ?)'  # '?' placeholder prevents sql injection.
        values = (
            directory_record.get('ProjectID'),
            directory_record.get('Name'),
            directory_record.get('Parent'),
        )
        cursor.execute(sql, values)

        # 3. Commit the transaction
        db_connection.commit()
    except Exception as e:
        raise Exception('Error connecting to target db') from e


def populate_import_files_table(db_connection, file_record: dict) -> None:
    """Writes a single record to the ImportFiles table of the target db.

    Args:
        db_connection (_type_): Db connection details.
        file_record (dict): FileName, DocumentID, ProjectID, ModifyDate, FolderPath, CreateDate, MD5, FileSize being written to the db.

    Raises:
        Exception: 'Error connecting to target db'
    """
    try:
        # 1. Establish connection
        cursor = db_connection.cursor()

        # 2. Execute Insert
        sql = 'INSERT INTO ImportFiles (FileName, DocumentID, ProjectID, ModifyDate, FolderPath, CreateDate, MD5, FileSize) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'  # '?' placeholder prevents sql injection.
        values = (
            file_record.get('FileName'),
            file_record.get('DocumentID'),
            file_record.get('ProjectID'),
            file_record.get('ModifyDate'),
            file_record.get('FolderPath'),
            file_record.get('CreateDate'),
            file_record.get('MD5'),
            file_record.get('FileSize'),
        )
        cursor.execute(sql, values)

        # 3. Commit the transaction
        db_connection.commit()
    except Exception as e:
        raise Exception('Error connecting to target db') from e
