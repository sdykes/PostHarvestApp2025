SELECT 
	SeasonDesc AS Season,
	bd.BinDeliveryID,
	BinDeliveryNo AS [Bin delivery No],
	FarmCode AS RPIN,
	FarmName AS Orchard,
	Grower,
	sbt.SubdivisionCode AS [Production site],
	fbt.BlockCode AS [Management area],
	HarvestDate AS [Harvest Date],
	NoOfBins AS [Bins received],
	COALESCE(BinsTipped,0) AS [Bins assigned for tipping],
	NoOfBins - COALESCE(BinsTipped,0) AS [Bins currently in storage],
	MaturityCode [Submission profile],
	PickNoDesc AS [Pick No],
	[Storage site],
	CASE
		WHEN StorageTypeID = 4 THEN 'CA'
		ELSE 'RA'
	END AS [Storage type],
	CASE
		WHEN StorageTypeID = 6 THEN 'No 1-MCP applied'
		ELSE '1-MCP applied'
	END AS SmartFreshed
FROM ma_Bin_DeliveryT bd
LEFT JOIN
	(
	SELECT 
		BinDeliveryID,
		COUNT(BinID) AS BinsTipped
	FROM ma_BinT AS bt
	WHERE bt.GraderBatchID IS NOT NULL
	GROUP BY BinDeliveryID
	) AS bt
ON bt.BinDeliveryID = bd.BinDeliveryID
INNER JOIN
	sw_SeasonT AS st
ON st.SeasonID = bd.SeasonID
INNER JOIN
	sw_FarmT AS ft
ON ft.FarmID = bd.FarmID
INNER JOIN
	sw_Farm_BlockT AS fbt
ON fbt.BlockID = bd.BlockID
INNER JOIN
	sw_SubdivisionT AS sbt
ON sbt.SubdivisionID = fbt.SubdivisionID
INNER JOIN
	sw_MaturityT AS mt
ON mt.MaturityID = bd.MaturityID
INNER JOIN
	sw_Pick_NoT AS pnt
ON pnt.PickNoID = bd.PickNoID
INNER JOIN
	(
	SELECT
		CompanyID,
		CompanyName AS [Storage site]
	FROM sw_CompanyT
	) AS cts
ON cts.CompanyID = bd.FirstStorageSiteCompanyID
INNER JOIN
	(
	SELECT
		CompanyID,
		CompanyName AS Grower
	FROM sw_CompanyT
	) AS cto
ON cto.CompanyID = ft.GrowerCompanyID
WHERE PresizeFlag = 0


