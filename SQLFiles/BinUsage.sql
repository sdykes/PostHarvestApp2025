WITH BinsTipped_CTE (BinDeliveryID, BinQty, GraderBatchID, BinsTipped)
AS
(	
	SELECT 
		BinDeliveryID,
		BinQty,
		bu.GraderBatchID,
		CASE
			WHEN ClosedDateTime IS NOT NULL THEN 1
			ELSE 0
		END AS BinsTipped
	FROM ma_Bin_UsageT AS bu
	INNER JOIN
		(
		SELECT
			GraderBatchID,
			ClosedDateTime
		FROM ma_Grader_BatchT
		) AS gb
	ON gb.GraderBatchID = bu.GraderBatchID
	WHERE bu.GraderBatchID IS NOT NULL
)
SELECT 
	BinDeliveryID,
	BinsTipped,
	SUM(BinQty) AS BinQty
FROM BinsTipped_CTE
GROUP BY BinDeliveryID, BinsTipped





