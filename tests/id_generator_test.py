from utils.id_generator import create_document_id, create_project_id


def test_create_project_id_increments():
    """Validates the following conditions for create_project_id:
    1. Each call returns a value one greater than the previous.

    """
    first = create_project_id()
    second = create_project_id()

    assert second == first + 1


def test_create_document_id_increments():
    """Validates the following conditions for create_document_id:
    1. Each call returns a value one greater than the previous.

    """
    first = create_document_id()
    second = create_document_id()

    assert second == first + 1


def test_create_project_id_returns_int():
    """Validates the following conditions for create_project_id:
    1. Returns an integer.

    """
    result = create_project_id()

    assert isinstance(result, int)


def test_create_document_id_returns_int():
    """Validates the following conditions for create_document_id:
    1. Returns an integer.

    """
    result = create_document_id()

    assert isinstance(result, int)
