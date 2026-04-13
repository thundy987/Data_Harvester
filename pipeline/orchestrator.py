from db.repository import populate_directories_table
from sources.windows_fs.scanner import walk_windows_fs
from utils.id_generator import create_project_id


def run_directories_pipeline(db_connection, scan_root_directory: str) -> dict:
    """Creates a dictionary that will be used to populate the Directories table
    with the folder path and a ProjectID.

    Args:
        db_connection (Connection): Connection details for the target db.
        scan_root_directory (str): The directory being harvested and loaded.

    Returns:
        lookup_dict (dict): A dictionary with {full_path: ProjectID} form.
    """
    _, all_folders = walk_windows_fs(scan_root_directory)

    sorted_folders = sorted(all_folders, key=lambda p: len(p.parts))

    lookup_dict = {}

    for folder in sorted_folders:
        try:
            directory_record = {'ProjectID': None, 'Name': '', 'Parent': ''}

            parent_folder = folder.parent
            directory_record['Parent'] = lookup_dict.get(parent_folder, 0)
            directory_record['ProjectID'] = create_project_id()
            directory_record['Name'] = folder.name

            populate_directories_table(db_connection, directory_record)

            lookup_dict[folder] = directory_record['ProjectID']
        except Exception as e:
            # TODO: replace with logger
            print(f'Skipping folder {folder}: {e}')
            continue
    return lookup_dict
