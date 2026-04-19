Select	
		d.Filename as FileName,
		p.Path as FolderPath
        
From Documents d
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