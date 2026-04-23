SELECT Name as FolderName, Path AS FolderPath
FROM Projects
WHERE Deleted = 0 AND Path IS NOT NULL