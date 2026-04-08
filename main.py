#from db.connection import connect_to_db
from sources.windows_fs.scanner import walk_windows_fs

#connect_to_db('sql\\dba', "Migration", "sa", "g")

walk_windows_fs('C:\\Data_Harvester')