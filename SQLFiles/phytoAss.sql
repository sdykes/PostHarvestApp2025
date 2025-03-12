SELECT 
	AssessmentDefectID,
	qad.AssessmentID,
	Season,
	GraderBatchID,
	GraderBatchMPILotID,
	Defect,
	DefectQty,
	SampleQty,
	dt.MktDefectCode	
FROM qa_Assessment_DefectT AS qad
INNER JOIN
	(
	SELECT
		DefectID,
		Defect,
		MktDefectCode
	FROM qa_DefectT
	) AS dt
ON dt.DefectID = qad.DefectID
INNER JOIN
	(
	SELECT
		AssessmentID,
		GraderBatchID,
		TemplateID,
		SampleQty,
		SeasonID,
		GraderBatchMPILotID
	FROM qa_AssessmentT
	) AS qa
ON qa.AssessmentID = qad.AssessmentID
INNER JOIN
	(
	SELECT
		SeasonID,
		SeasonDesc AS Season
	FROM sw_SeasonT
	) AS st
ON st.SeasonID = qa.SeasonID
INNER JOIN
	(
	SELECT 
		DISTINCT pmr.PIPReqID,
		pipr.MktDefectCode,
		pipr.DeclarationDesc
	FROM pip_Market_RequirementT AS pmr
	LEFT JOIN
		(
		SELECT 
			MktDefectCode,
			prp.PIPReqID,
			PercentLimit,
			ThresholdQty,
			DeclarationDesc
		FROM pip_Requirement_PestT AS prp
		LEFT JOIN
			pip_RequirementT AS pr
		ON pr.PIPReqID = prp.PIPReqID
		) AS pipr
	ON pipr.PIPReqID = pmr.PIPReqID
	WHERE PIPMarketCode IN ('CHN','TWN')
	AND ThresholdQty = 0.0000
	) pip
ON pip.MktDefectCode = dt.MktDefectCode
WHERE dt.MktDefectCode IS NOT NULL
AND TemplateID = 10

