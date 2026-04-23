from pathlib import Path

from db.connection import connect_to_db
from sources.base import SourceSystem
from utils.logger import logger

_SQL_DIR = Path(__file__).parent / 'sql'


class PDMDatabase(SourceSystem):
    def __init__(self, server_name: str, db_name: str, username: str, password: str):
        """Initializes the PDMDatabase source with connection credentials. Connection is deferred until fetch_data() is called.

        Args:
            server_name (str): SQL Server and instance name (e.g. server\\instance).
            db_name (str): Name of the PDM vault database.
            username (str): SQL Server username.
            password (str): Password for the supplied username.
        """
        self._source_location = f'{server_name}\\{db_name}'
        self.server_name = server_name
        self.db_name = db_name
        self.username = username
        self.password = password

    @property
    def source_location(self) -> str:
        """
        Server and database name provided at instantiation. Returns the private backing variable to satisfy the SourceSystem abstract property contract.
        """
        return self._source_location

    def fetch_data(
        self,
    ) -> dict[str, list[dict]]:
        """Connects to the PDM vault database and queries the Projects and Documents tables to build directory and file records.

        Raises:
            RuntimeError: If the database query fails.

        Returns:
            dict[str, list[dict]]: A dictionary with 'folders' and 'files' keys, each containing a list of record dicts.
        """
        db_connection = None  # guard against 'finally' running if connection fails
        try:
            db_connection = connect_to_db(
                self.server_name, self.db_name, self.username, self.password
            )
            with db_connection.cursor() as cursor:
                # Query the Projects table and build directory_records[]
                sql_folders = (_SQL_DIR / 'get_folders.sql').read_text()
                cursor.execute(sql_folders)

                directory_records = []

                for row in cursor:
                    try:
                        if row.FolderPath == '\\':
                            directory_records.append(
                                {
                                    'Path': Path(
                                        row.FolderName
                                    ),  # convert to Path object for pipeline
                                    'Name': row.FolderName,
                                }
                            )
                        else:
                            directory_records.append(
                                {
                                    'Path': Path(
                                        row.FolderPath.removesuffix('\\')
                                    ),  # convert to Path object for pipeline
                                    'Name': row.FolderName,
                                }
                            )
                    except Exception as e:
                        logger.warning(f'Skipping folder {row.FolderPath}: {e}')
                        continue
                # Query the Documents table and build file_records[]
                sql_files = (_SQL_DIR / 'get_files.sql').read_text()

                cursor.execute(sql_files)

                file_records = []

                for row in cursor:
                    try:
                        if row.FolderPath == '\\':
                            file_records.append(
                                {
                                    'FileName': row.FileName,
                                    'ModifyDate': row.ModifyDate.strftime(
                                        '%Y-%m-%dT%H:%M:%S.%f'
                                    )[:-3],
                                    'FolderPath': row.FolderName,
                                    'CreateDate': row.CreateDate.strftime(
                                        '%Y-%m-%dT%H:%M:%S.%f'
                                    )[:-3],
                                    'MD5': None,
                                    'FileSize': row.FileSize,
                                }
                            )
                        else:
                            file_records.append(
                                {
                                    'FileName': row.FileName,
                                    'ModifyDate': row.ModifyDate.strftime(
                                        '%Y-%m-%dT%H:%M:%S.%f'
                                    )[:-3],
                                    'FolderPath': row.FolderPath.removesuffix('\\'),
                                    'CreateDate': row.CreateDate.strftime(
                                        '%Y-%m-%dT%H:%M:%S.%f'
                                    )[:-3],
                                    'MD5': None,
                                    'FileSize': row.FileSize,
                                }
                            )
                    except Exception as e:
                        logger.warning(f'Skipping file {row.FileName}: {e}')
                        continue
            return {'folders': directory_records, 'files': file_records}
        except Exception as e:
            logger.error(f'Failed to query the {self.db_name} database: {e}')
            raise RuntimeError(f'Failed to query the {self.db_name} database') from e
        finally:
            if db_connection:
                db_connection.close()
