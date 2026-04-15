import os
import pytest
from dotenv import load_dotenv
from db.connection import connect_to_db
from db.repository import clear_files_and_folders_tables

load_dotenv()


@pytest.fixture(scope='function')
def db_connection():
    """
    Provides a live DB connection for integration tests.
    Wipes Directories and ImportFiles before and after each test.
    """
    conn = connect_to_db(
        server_name=os.getenv('SERVER'),
        db_name=os.getenv('DATABASE'),
        username=os.getenv('SQL_USERNAME'),
        password=os.getenv('PASSWORD'),
    )
    clear_files_and_folders_tables(conn)
    yield conn
    clear_files_and_folders_tables(conn)
    conn.close()


@pytest.fixture(scope='session')
def dummy_data_dir():
    """Returns the absolute path to the dummy_data folder."""
    return os.path.join(os.path.dirname(__file__), 'dummy_data')
