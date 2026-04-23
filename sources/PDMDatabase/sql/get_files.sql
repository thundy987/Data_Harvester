WITH   Columns
AS     (SELECT d.Filename AS FileName,
               p.Name AS FolderName,
               p.Path AS FolderPath,
               r.RevNR AS versions,
               FIRST_VALUE(r.Date) OVER (PARTITION BY d.DocumentID ORDER BY r.RevNR ASC) AS CreateDate,
               FIRST_VALUE(r.Date) OVER (PARTITION BY d.DocumentID ORDER BY r.RevNR DESC) AS ModifyDate,
               FIRST_VALUE(r.FileSize) OVER (PARTITION BY d.DocumentID ORDER BY r.RevNR DESC) AS FileSize
        FROM   Revisions AS r
               INNER JOIN
               Documents AS d
               ON r.DocumentID = d.DocumentID
               INNER JOIN
               DocumentsInProjects AS dp
               ON d.DocumentID = dp.DocumentID
               INNER JOIN
               Projects AS p
               ON dp.ProjectID = p.ProjectID
               INNER JOIN
               ObjectType AS ot
               ON d.ObjectTypeID = ot.ObjectTypeID
        WHERE  ot.Description = 'Normal Document'
               AND d.Deleted = 0
               AND EXISTS (SELECT 1
                           FROM   DocumentsInProjects AS dip
                                  INNER JOIN
                                  Projects AS p
                                  ON dip.ProjectID = p.ProjectID
                           WHERE  dip.DocumentID = d.DocumentID
                                  AND dip.Deleted = 0
                                  AND p.Deleted = 0))
SELECT DISTINCT Foldername,
                FileName,
                ModifyDate,
                FolderPath,
                CreateDate,
                FileSize
FROM   Columns;

