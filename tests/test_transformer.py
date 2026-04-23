from pipeline.transformer import (
    find_illegal_characters_in_file_name,
    format_unix_date_to_iso,
    remove_white_spaces,
)


def test_remove_white_spaces():
    """Validates the following conditions for remove_white_spaces:
    1. Leadings spaces removed.
    2. Trailing spaces removed.
    3. Internal spaces preserved.
    4. None returns None.
    Args:
        text (str): text that has leading/trailing spaces.


    """
    assert remove_white_spaces('   abc') == 'abc'
    assert remove_white_spaces('abc   ') == 'abc'
    assert remove_white_spaces('a b c') == 'a b c'
    assert remove_white_spaces(None) is None


def test_find_illegal_characters_in_file_name() -> list | None:
    """Validates the following conditions for find_illegal_characters_in_file_name():
    1. A clean filename returns None.
    2. A filename with an illegal character returns a list containing the character.
    3. A filename ending with '.' returns a list containing 'trailing period'.

    Args:
        file_name (str): name of the file


    """
    assert find_illegal_characters_in_file_name('myfile.txt') is None
    assert find_illegal_characters_in_file_name('<') == ['<']
    assert find_illegal_characters_in_file_name('myfile.') == ['trailing period']


def test_format_unix_date_to_iso() -> str | None:
    """Validates that a unix date format gets converted to iso601 date format

    Args:
        ugly_date (float): Unix timestamp

    Returns:
        formatted_date (str): Date string with format of yyyy-MM-ddTHH:mm:ss.fff
        None: If the timestamp is invalid (e.g. negative, infinite, or out of range)
    """
    assert format_unix_date_to_iso(1774915205.010) == '2026-03-30T19:00:05.010'
    assert format_unix_date_to_iso(-1774915205.010) is None
