import argparse
import os

from dotenv import load_dotenv

from db.connection import connect_to_db
from db.repository import (
    clear_files_and_folders_tables,
    is_directories_empty,
    is_import_files_empty,
)
from pipeline.orchestrator import run_pipeline
from sources.PDMDatabase.PDMDatabase import PDMDatabase
from utils.logger import logger

load_dotenv()


def main():
    parser = argparse.ArgumentParser(
        description='Script scans a data source, cleanses data, and loads it into a database'
    )

    parser.add_argument(
        '--source_location',
        help='the source of the data to be scanned',
        default=os.getenv('ROOT'),
    )
    parser.add_argument(
        '--server_name', help='server\\instance name', default=os.getenv('SERVER')
    )
    parser.add_argument(
        '--database_name', help='target db name', default=os.getenv('DATABASE')
    )
    parser.add_argument(
        '--sql_username',
        help='sql server account name: default is sa',
        default=os.getenv('SQL_USERNAME'),
    )
    parser.add_argument(
        '--password',
        help='password for the provided user acct',
        default=os.getenv('PASSWORD'),
    )
    parser.add_argument('--clear', action='store_true', help='clears database tables')
    parser.add_argument('--batch_size', type=int, default=1000)

    args = parser.parse_args()

    if args.batch_size <= 0:
        logger.error(f'Invalid batch size: {args.batch_size}')
        raise ValueError(
            f'Batch size must be a positive integer, got {args.batch_size}'
        )

    db_connection = None  # guard against 'finally' running if connection fails
    try:
        db_connection = connect_to_db(
            args.server_name, args.database_name, args.sql_username, args.password
        )

        if args.clear:
            clear_files_and_folders_tables(db_connection)

        if not is_directories_empty(db_connection):
            raise RuntimeError(
                'Directories table is not empty. Re-run with --clear to clear tables.'
            )

        if not is_import_files_empty(db_connection):
            raise RuntimeError(
                'ImportFiles table is not empty. Re-run with --clear to clear tables.'
            )

        # source = WindowsFS(args.source_location)
        source = PDMDatabase('sql\\dba', 'TSQLDemo', 'sa', 'g')

        run_pipeline(db_connection, args.batch_size, source)

    finally:
        if db_connection:
            db_connection.close()


if __name__ == '__main__':
    main()
