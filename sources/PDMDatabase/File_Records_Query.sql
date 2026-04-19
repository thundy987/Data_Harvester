;With Columns as(
	Select	
		d.Filename as FileName,
		p.Path as FolderPath,
		r.RevNR as versions,
        -- Get the date for the lowest RevNR
        FIRST_VALUE(r.Date) OVER (PARTITION BY d.DocumentID ORDER BY r.RevNR ASC) as CreateDate,
        -- Get the date and size for the highest RevNR
        FIRST_VALUE(r.Date) OVER (PARTITION BY d.DocumentID ORDER BY r.RevNR DESC) as ModifyDate,
        FIRST_VALUE(r.FileSize) OVER (PARTITION BY d.DocumentID ORDER BY r.RevNR DESC) as FileSize
	From Revisions r
	INNER JOIN Documents d
		ON r.DocumentID = d.DocumentID
	INNER JOIN DocumentsInProjects dp
		ON d.DocumentID = dp.DocumentID
	INNER JOIN Projects p
		ON dp.ProjectID = p.ProjectID
	INNER JOIN ObjectType ot
    ON d.ObjectTypeID = ot.ObjectTypeID
WHERE
    ot.Description = 'Normal Document'
    AND d.Deleted = 0
    AND EXISTS (
        SELECT 1
        FROM DocumentsInProjects dip
        INNER JOIN Projects p
            ON dip.ProjectID = p.ProjectID
        WHERE
            dip.DocumentID = d.DocumentID
            AND dip.Deleted = 0
            AND p.Deleted = 0
        )
)

Select DISTINCT
	FileName,
    ModifyDate,
	FolderPath,
    CreateDate,
    FileSize
From Columns