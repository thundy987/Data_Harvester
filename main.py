from db.connection import connect_to_db

# from sources.windows_fs.scanner import walk_windows_fs
# from sources.windows_fs.metadata import extract_file_properties
# from pipeline.transformer import cleanse_file_handler
from pipeline.orchestrator import run_directories_pipeline

db_connection = connect_to_db('sql\\dba', 'Migration', 'sa', 'g')
# walk_windows_fs('C:\\Data_Harvester')
# props = extract_file_properties('C:\\Temp\\context.md')
# print(props)

# props_cleaned = cleanse_file_handler(props)
# print(props_cleaned)

run_directories_pipeline(db_connection, 'C:\\Projects')

# TODO close db connection
