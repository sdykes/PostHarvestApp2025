WITH defAss (Season,RPIN,Orchard,[Production site],[Management area],GraderBatchID,Defect,DefectQty)
AS
	 (
	 SELECT 
		SeasonDesc AS Season,
		FarmCode AS RPIN,
		FarmName AS Orchard,
		SubdivisionCode AS [Production site],
		BlockCode AS [Management area],
		GraderBatchID,
		Defect,
		SUM(DefectQty) AS DefectQty
	FROM qa_Assessment_DefectT AS qad
	INNER JOIN
		qa_AssessmentT AS qa
	ON qa.AssessmentID = qad.AssessmentID
	INNER JOIN
		qa_DefectT AS qd
	ON qd.DefectID = qad.DefectID
	INNER JOIN
		sw_FarmT AS ft
	ON ft.FarmID = qa.FarmID
	INNER JOIN
		sw_Farm_BlockT AS fbt
	ON fbt.BlockID = qa.BlockID
	INNER JOIN
		sw_SubdivisionT AS sbt
	ON sbt.SubdivisionID = fbt.SubdivisionID
	INNER JOIN
		sw_SeasonT AS st
	ON st.SeasonID = qa.SeasonID
	WHERE TemplateID IN (13,14,28)
	GROUP BY SeasonDesc,FarmCode,FarmName,SubdivisionCode,BlockCode,GraderBatchID,Defect 
	) 
SELECT 
	Season,
	RPIN,
	Orchard,
	[Production site],
	[Management area],
	da.GraderBatchID,
	Defect,
	DefectQty,
	SampleQty
FROM defAss AS da
LEFT JOIN
	(
	SELECT 
		GraderBatchID,
		SUM(SampleQty) AS SampleQty
	FROM qa_AssessmentT
	WHERE TemplateID IN (13,14,28)
	GROUP BY GraderBatchID
	) AS sampq
ON sampq.GraderBatchID = da.GraderBatchID



