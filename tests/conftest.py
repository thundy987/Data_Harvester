import os
import pytest
from dotenv import load_dotenv

load_dotenv()


@pytest.fixture(scope='session')
def dummy_data_dir():
    """Returns the absolute path to the dummy_data folder."""
    return os.path.join(os.path.dirname(__file__), 'dummy_data')
