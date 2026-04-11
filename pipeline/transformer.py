from datetime import datetime


def cleanse_file_handler(raw_record: tuple) -> dict:
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
    illegal = find_illegal_characters_in_file_name(file_name)
    if illegal:
        # TODO: replace with logger
        print(f'{file_name} contains illegal characters: {illegal}')

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


def remove_white_spaces(text: str) -> str:
    """Removes any leading and trailing spaces from a str.

    Args:
        text (str): text that has leading/trailing spaces.

    Returns:
        str: text with no leading/trailing spaces.
    """
    if text is None:
        return None
    return text.strip()


def find_illegal_characters_in_file_name(file_name: str) -> list:
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
        # TODO: log that the file ends with a period and it illegal
        illegal_characters.append('trailing period')

    if illegal_characters:
        # TODO: log that an illegal character was found in file name.
        return illegal_characters
    return None


def format_date(ugly_date: float) -> str:
    """Converts a Unix timestamp (float) to ISO 8601 date format (str).

    Args:
        ugly_date (float): Unix timestamp

    Returns:
        formatted_date (str): Date string with format of yyyy-MM-ddTHH:mm:ss.fff
    """
    try:
        clean_date = datetime.fromtimestamp(ugly_date)
        formatted_date = clean_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
        return formatted_date
    except Exception:
        # TODO: replace print with logger
        print('Issue found during date formatting')
        return None
