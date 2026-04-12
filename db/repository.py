def populate_directories_table(db_connection, directory_record: dict) -> None:
    """Writes a single record to the Directories table of the target db

    Args:
        db_connection (Connection): Db connection details.
        directory_record (dict): ProjectID, Name, Parent being written to the db.
    """
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
