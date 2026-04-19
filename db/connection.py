import pyodbc

from utils.logger import logger


def connect_to_db(server_name: str, db_name: str, username: str, password: str):
    """
    Attempts to connect to a SQL Server instance using supplied info.
    Args:
        server_name (str): SQL server and instance name (e.g. server\\instance).
        db_name (str): name of database to connect to.
        username (str): SQL Server username (not windows auth).
        password (str): password for supplied username.
    Raises:
        ConnectionError: If the database connection cannot be established.
    Returns:
        Connection (Connection): connection string for pyodbc.
    """
    try:
        connection = pyodbc.connect(
            f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server_name};DATABASE={db_name};UID={username};PWD={password};TrustServerCertificate=yes'
        )

        logger.info('Successfully connected to database')
        return connection
    except pyodbc.Error as e:
        logger.error(f'Failed to connect to {server_name}/{db_name}: {e}')
        raise ConnectionError(f'Unable to connect to {server_name}/{db_name}') from e
