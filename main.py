from pipeline.pipeline import run_pipeline
from ui.cli_prompt import cli_setup
from utils.logger import logger


def main():
    db_connection = None
    try:
        # One call handles all interactive logic and returns the live connection
        db_connection, batch, source = cli_setup()

        run_pipeline(db_connection, batch, source)
    except SystemExit:
        pass
    except Exception as e:
        logger.error(f'Application error: {e}')
    finally:
        if db_connection:
            db_connection.close()
            print('Database connection closed.')


if __name__ == '__main__':
    main()
