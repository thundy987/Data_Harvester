from unittest.mock import MagicMock, patch

import pyodbc
import pytest

from db.connection import connect_to_db


@patch('db.connection.pyodbc.connect')
def test_connect_to_db_success(mock_connect):
    """Validates the following conditions for connect_to_db:
    1. A successful connection returns the connection object.

    Args:
        mock_connect (MagicMock): Mock replacing pyodbc.connect.
    """
    # Create a fake connection object to replace the return of pyodbc.connect.
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection

    # Call the real function with dummy args, assert that it returns fake connection object.
    result = connect_to_db('server\\instance', 'mydb', 'user', 'pass')

    assert result == mock_connection


@patch('db.connection.pyodbc.connect')
def test_connect_to_db_raises_connection_error(mock_connect):
    """Validates the following conditions for connect_to_db:
    1. A pyodbc.Error raised by pyodbc.connect results in a ConnectionError.

    Args:
        mock_connect (MagicMock): Mock replacing pyodbc.connect.
    """
    mock_connect.side_effect = pyodbc.Error('connection failed')

    with pytest.raises(ConnectionError):
        connect_to_db('server\\instance', 'mydb', 'user', 'pass')
