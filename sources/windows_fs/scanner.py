from pathlib import Path

def walk_windows_fs (scan_root_directory):
    if not Path(scan_root_directory).exists():
         raise Exception('User supplied an invalid root path')  
    try:
        collection = list(Path(scan_root_directory).rglob("*"))
        
        file_list = [f for f in collection if f.is_file()]
        
        folder_list = [f for f in collection if f.is_dir()]
        print(file_list)
    #    print(folder_list)
        return file_list, folder_list
    except Exception as e:
        raise Exception('Error occurred during file scan') from e