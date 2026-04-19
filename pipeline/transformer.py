from datetime import datetime
from pathlib import Path

from utils.logger import logger


def cleanse_file_record(
    raw_record: tuple[Path, str, str, float, float, int, str],
) -> dict:
    """Executes cleansing functions on a tuple of file properties and returns the values as a dictionary.

    Args:
        raw_record (tuple): metadata of a harvested file

    Returns:
        dict: metadata properties defined as dictionary
    """
    (
        parent_folder,
        file_name,
        file_extension,
        create_date,
        last_modified_date,
        file_size,
        md5_hash,
    ) = raw_record

    file_name = remove_white_spaces(file_name)

    if file_name:  # make sure file_name is not None
        illegal = find_illegal_characters_in_file_name(file_name)
        if illegal:
            logger.warning(f'{file_name} contains illegal characters: {illegal}')

    create_date = format_date(create_date)
    last_modified_date = format_date(last_modified_date)

    # convert tuple to dictionary to use upstream
    return {
        'FolderPath': str(parent_folder),
        'FileName': file_name,
        'file_extension': file_extension,  # variable not used by migration db, left it as snake_case format.
        'CreateDate': create_date,
        'ModifyDate': last_modified_date,
        'FileSize': file_size,
        'MD5': md5_hash,
    }


def remove_white_spaces(text: str) -> str | None:
    """Removes any leading and trailing spaces from a str.

    Args:
        text (str): text that has leading/trailing spaces.

    Returns:
        str: text with no leading/trailing spaces.
    """
    if text is None:
        return None
    return text.strip()


def find_illegal_characters_in_file_name(file_name: str) -> list | None:
    """Checks for illegal characters in a given file name for a Windows file system.

    Args:
        file_name (str): name of the file

    Returns:
        list: A list of the illegal characters found in the name.
    """
    illegal_characters = [
        n for n in file_name if n in ['<', '>', ':', '"', '/', '\\', '|', '?', '*']
    ]

    if file_name.endswith('.'):
        illegal_characters.append('trailing period')
        logger.warning(f'{file_name} has a trailing period')

    if illegal_characters:
        logger.warning(f'{file_name} contains illegal characters: {illegal_characters}')
        return illegal_characters

    return None


def format_date(ugly_date: float) -> str | None:
    """Converts a Unix timestamp (float) to ISO 8601 date format (str).

    Args:
        ugly_date (float): Unix timestamp

    Returns:
        formatted_date (str): Date string with format of yyyy-MM-ddTHH:mm:ss.fff
        None: If the timestamp is invalid (e.g. negative, infinite, or out of range)
    """
    try:
        clean_date = datetime.fromtimestamp(ugly_date)
        formatted_date = clean_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
        return formatted_date
    except (ValueError, OSError, OverflowError) as e:
        logger.warning(f'Failed to format timestamp {ugly_date}: {e}')
        return None
