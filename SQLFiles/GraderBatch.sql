SELECT 
	gb.GraderBatchID,
	GraderBatchNo AS [Grader Batch],
	SeasonDesc AS Season,
	Grower,
	FarmCode AS RPIN,
	FarmName AS Orchard,
	SubdivisionCode AS [Production site],
	HarvestDate AS [Harvest date],
	PackDate AS [Pack date],
	[Packing site],
	[Bins tipped],
	InputKgs AS [Input kgs],
	WasteOtherKgs + COALESCE(JuiceKgs,0) + COALESCE(SampleKgs,0) AS [Reject kgs],
	CASE	
		WHEN ClosedDateTime IS NULL THEN 0
		ELSE 1
	END AS [Batch closed]
FROM ma_Grader_BatchT AS gb
INNER JOIN
	sw_SeasonT AS st
ON st.SeasonID = gb.SeasonID
INNER JOIN
	sw_FarmT AS ft
ON ft.FarmID = gb.FarmID
INNER JOIN
	sw_SubdivisionT AS sbt
ON sbt.SubdivisionID = gb.SubdivisionID
INNER JOIN
	sw_MaturityT AS mt
ON mt.MaturityID = gb.MaturityID
INNER JOIN
	sw_Pick_NoT AS pnt
ON pnt.PickNoID = gb.PickNoID
INNER JOIN
	(
	SELECT
		CompanyID,
		CompanyName AS [Packing site]
	FROM sw_CompanyT
	) AS ctp
ON ctp.CompanyID = gb.PackingCompanyID
INNER JOIN
	(
	SELECT
		CompanyID,
		CompanyName AS Grower
	FROM sw_CompanyT
	) AS cto
ON cto.CompanyID = ft.GrowerCompanyID
LEFT JOIN
	(
	SELECT
		PresizeOutputFromGraderBatchID AS GraderBatchID,
		SUM(TotalWeight) AS JuiceKgs
	FROM ma_Bin_DeliveryT
	WHERE PresizeProductID = 278
	GROUP BY PresizeOutputFromGraderBatchID
	) AS jk
ON jk.GraderBatchID = gb.GraderBatchID
LEFT JOIN
	(
	SELECT 
		GraderBatchID,
		NoOfUnits*NetFruitWeight AS SampleKgs
	FROM ma_Pallet_DetailT AS pd
	INNER JOIN
		(
		SELECT
			ProductID,
			NetFruitWeight
		FROM sw_ProductT
		WHERE SampleFlag = 1
		) AS pt
	ON pt.ProductID = pd.ProductID
	) AS sk
ON sk.GraderBatchID = gb.GraderBatchID
LEFT JOIN
	(
	SELECT 
		GraderBatchID,
		COUNT(BinID) AS [Bins tipped]
	FROM ma_BinT
	WHERE GraderBatchID IS NOT NULL
	GROUP BY GraderBatchID
	) AS bt
ON bt.GraderBatchID = gb.GraderBatchID


