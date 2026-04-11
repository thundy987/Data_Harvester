from itertools import count

id_gen = count(start=1)


def create_project_id() -> int:
    """Generates an incrementing counter.

    Returns:
        int: Unique counter ID for projectID
    """
    return next(id_gen)


def create_document_id() -> int:
    """Generates an incrementing counter.

    Returns:
        int: Unique counter ID for DocumentID
    """
    return next(id_gen)
