"""
Integration tests against the live SQL Server instance defined in .env.
The db_connection fixture (conftest.py) truncates both tables before AND after
each test, so these tests are safe to run repeatedly.
"""
import os
import unittest
import tempfile
import shutil
from pathlib import Path

import pytest
from dotenv import load_dotenv

from db.connection import connect_to_db
from db.repository import (
    clear_files_and_folders_tables,
    is_directories_empty,
    is_import_files_empty,
    populate_directories_table,
    populate_import_files_table,
    log_activity,
)
from pipeline.orchestrator import run_pipeline

load_dotenv()


@pytest.fixture(scope='function')
def conn():
    """Live DB connection. Tables are wiped before and after every test."""
    connection = connect_to_db(
        server_name=os.getenv('SERVER'),
        db_name=os.getenv('DATABASE'),
        username=os.getenv('SQL_USERNAME'),
        password=os.getenv('PASSWORD'),
    )
    clear_files_and_folders_tables(connection)
    yield connection
    clear_files_and_folders_tables(connection)
    connection.close()


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------
class TestLiveConnection:

    def test_connect_succeeds(self, conn):
        assert conn is not None

    def test_bad_credentials_raise_exception(self):
        with pytest.raises(Exception, match='Unable to connect to database instance'):
            connect_to_db('sql\\dba', 'Migration', 'sa', 'wrong_password')


# ---------------------------------------------------------------------------
# Table state checks
# ---------------------------------------------------------------------------
class TestTableStateChecks:

    def test_directories_empty_after_clear(self, conn):
        assert is_directories_empty(conn) is True

    def test_import_files_empty_after_clear(self, conn):
        assert is_import_files_empty(conn) is True

    def test_directories_not_empty_after_insert(self, conn):
        populate_directories_table(conn, {'ProjectID': 1, 'Name': 'Root', 'Parent': 0})
        assert is_directories_empty(conn) is False

    def test_import_files_not_empty_after_insert(self, conn):
        populate_directories_table(conn, {'ProjectID': 1, 'Name': 'Root', 'Parent': 0})
        populate_import_files_table(conn, {
            'FileName': 'test.txt',
            'DocumentID': 1,
            'ProjectID': 1,
            'ModifyDate': '2024-01-01T00:00:00.000',
            'FolderPath': 'C:/root',
            'CreateDate': '2024-01-01T00:00:00.000',
            'MD5': 'abc123',
            'FileSize': 100,
        })
        assert is_import_files_empty(conn) is False


# ---------------------------------------------------------------------------
# Populate tables
# ---------------------------------------------------------------------------
class TestPopulateTables:

    def test_insert_directory_record(self, conn):
        populate_directories_table(conn, {'ProjectID': 1, 'Name': 'Root', 'Parent': 0})
        assert is_directories_empty(conn) is False

    def test_insert_file_record(self, conn):
        populate_directories_table(conn, {'ProjectID': 1, 'Name': 'Root', 'Parent': 0})
        populate_import_files_table(conn, {
            'FileName': 'test.txt',
            'DocumentID': 1,
            'ProjectID': 1,
            'ModifyDate': '2024-01-01T00:00:00.000',
            'FolderPath': 'C:/root',
            'CreateDate': '2024-01-01T00:00:00.000',
            'MD5': 'abc123',
            'FileSize': 100,
        })
        assert is_import_files_empty(conn) is False

    def test_clear_empties_both_tables(self, conn):
        populate_directories_table(conn, {'ProjectID': 1, 'Name': 'Root', 'Parent': 0})
        clear_files_and_folders_tables(conn)
        assert is_directories_empty(conn) is True
        assert is_import_files_empty(conn) is True


# ---------------------------------------------------------------------------
# log_activity stored procedure
# ---------------------------------------------------------------------------
class TestLogActivityIntegration:

    def test_log_activity_does_not_raise(self, conn):
        log_activity(conn, 'Integration test activity')

    def test_log_activity_with_timestamp_does_not_raise(self, conn):
        from datetime import datetime
        log_activity(conn, 'Timed activity', activity_time=datetime.now())


# ---------------------------------------------------------------------------
# Full pipeline
# ---------------------------------------------------------------------------
class TestRunPipelineIntegration:

    def setup_method(self):
        """Create a small temp directory tree to scan."""
        self.tmp = tempfile.mkdtemp()
        subdir = Path(self.tmp) / 'subdir'
        subdir.mkdir()
        (Path(self.tmp) / 'file_a.txt').write_text('hello')
        (subdir / 'file_b.csv').write_text('col1,col2\n1,2')

    def teardown_method(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_pipeline_populates_directories(self, conn):
        run_pipeline(conn, self.tmp)
        assert is_directories_empty(conn) is False

    def test_pipeline_populates_import_files(self, conn):
        run_pipeline(conn, self.tmp)
        assert is_import_files_empty(conn) is False

    def test_pipeline_raises_if_directories_not_empty(self, conn):
        """Second run without --clear should raise."""
        run_pipeline(conn, self.tmp)
        # Do NOT clear — simulate user forgetting --clear
        with pytest.raises(Exception):
            run_pipeline(conn, self.tmp)

    def test_pipeline_with_invalid_root_raises(self, conn):
        with pytest.raises(Exception):
            run_pipeline(conn, 'C:/this/path/does/not/exist/xyz')
