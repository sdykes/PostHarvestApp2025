SELECT 
	GraderBatchMPILotID,
	gbml.GraderBatchID,
	ctg.CompanyName AS Grower,
	FarmCode AS RPIN,
	FarmName AS Orchard,
	SubdivisionCode AS [Production site],
	ctp.CompanyName AS [Packing site]
FROM ma_Grader_Batch_MPI_LotT AS gbml
INNER JOIN
	ma_Grader_BatchT AS gb
ON gb.GraderBatchID = gbml.GraderBatchID
INNER JOIN
	sw_FarmT AS ft
ON ft.FarmID = gb.FarmID
INNER JOIN
	sw_SubdivisionT AS sbt
ON sbt.SubdivisionID = gb.SubdivisionID
INNER JOIN
	sw_CompanyT AS ctg
ON ctg.CompanyID = gb.GrowerCompanyID
INNER JOIN
	sw_CompanyT AS ctp
ON ctp.CompanyID = gb.PackingCompanyID
INNER JOIN
	sw_SeasonT AS st
ON st.SeasonID = gb.SeasonID
