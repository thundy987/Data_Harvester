from rich.console import Console
from rich.prompt import Confirm, IntPrompt, Prompt

from db.connection import connect_to_db
from db.repository import (
    clear_files_and_folders_tables,
    is_directories_empty,
    is_import_files_empty,
)
from sources.PDMDatabase.PDMDatabase import PDMDatabase
from sources.windows_fs.WindowsFS import WindowsFS

console = Console()


def get_db_creds(label='Target Database'):
    console.print(f'\n[bold blue]{label} Setup[/]')

    # Apply defaults to destination server info.
    is_target = label == 'Target SQL Server'
    return {
        'server': Prompt.ask('Server Name'),
        'database': Prompt.ask(
            'Database Name', default='Migration' if is_target else None
        ),
        'username': Prompt.ask('Username', default='sa' if is_target else None),
        'password': Prompt.ask('Password', password=True),
    }


def cli_setup():
    # Add sources to the list as needed.
    src_type = Prompt.ask('Select source', choices=['Windows Folder', 'PDM Database'])

    source = None
    while source is None:
        match src_type:
            case 'Windows Folder':
                path = Prompt.ask('Root folder path')
                try:
                    # WindowsFS raises FileNotFoundError if path is invalid
                    source = WindowsFS(path)
                    console.print(f'[bold green]✔ Path verified:[/] {path}')
                except Exception as e:
                    console.print(f'\n[bold red]Invalid Path:[/] {e}')
                    if not Confirm.ask('Would you like to try a different path?'):
                        raise SystemExit('Aborted: Invalid source path.')

            case 'PDM Database':
                # Matching label exactly for the default logic below
                c = get_db_creds('Source PDM')
                try:
                    temp_conn = connect_to_db(
                        c['server'], c['database'], c['username'], c['password']
                    )
                    temp_conn.close()
                    source = PDMDatabase(
                        c['server'], c['database'], c['username'], c['password']
                    )
                    console.print('[bold green]✔ Source Connection Successful![/]')
                except Exception as e:
                    console.print(f'\n[bold red]Source Connection Failed:[/] {e}')
                    if not Confirm.ask('Retry source credentials?'):
                        raise SystemExit('Aborted: Source connection failed.')

            case _:
                # Fallback
                raise ValueError(f'Unknown source type: {src_type}')

    # Target Connection handling
    db_connection = None
    while db_connection is None:
        target_creds = get_db_creds('Target SQL Server')
        try:
            db_connection = connect_to_db(
                target_creds['server'],
                target_creds['database'],
                target_creds['username'],
                target_creds['password'],
            )
            console.print('[bold green]✔ Target Connection Successful![/]')

        except Exception as e:
            console.print(f'\n[bold red]Target Connection Failed:[/] {e}')
            if not Confirm.ask('Retry target credentials?'):
                raise SystemExit('Aborted: Target connection failed.')

    # Table Check & Clear
    tables_dirty = not is_directories_empty(db_connection) or not is_import_files_empty(
        db_connection
    )

    if tables_dirty:
        if Confirm.ask(
            '[yellow]Target tables are not empty.[/] Clear migration tables now?'
        ):
            clear_files_and_folders_tables(db_connection)
            console.print('[green]Tables cleared.[/]')
        else:
            db_connection.close()
            console.print('[red]Aborting: Tables must be empty to proceed.[/]')
            raise SystemExit('Migration halted: User chose not to clear existing data.')

    # Batch Size
    batch_size = IntPrompt.ask('Enter batch size', default=1000)
    while batch_size <= 0:
        console.print('[red]Batch size must be a positive integer.[/]')
        batch_size = IntPrompt.ask('Enter batch size', default=1000)

    return db_connection, batch_size, source
