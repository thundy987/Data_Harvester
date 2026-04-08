import pyodbc

def connect_to_db (server_name:str, db_name:str, username:str, password:str):
    """
    Attempts to connect to a SQL Server instance using supplied info. Raises exception if connection fails
    Args:
        server_name (str): SQL server and instance name (e.g. server\instance).
        db_name (str): name of database to connect to.
        username (str): SQL Server username (not windows auth).   
        password (str): password for supplied username.
    Returns:
        Connection (Connection): connection string for pyodbc.
    """
    try:
        connection = pyodbc.connect(f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server_name};DATABASE={db_name};UID={username};PWD={password};TrustServerCertificate=yes')
        print('Successfully connected to database')
        return connection
    except pyodbc.Error as e:
        raise  Exception('Unable to connect to database instance.') from e