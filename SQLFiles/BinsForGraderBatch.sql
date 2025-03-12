SELECT 
	SeasonDesc AS Season,
	bt.GraderBatchID,
	BinDeliveryID,
	COUNT(BinID) AS BinsTipped
FROM ma_BinT AS bt
LEFT JOIN
	ma_Grader_BatchT AS gb
ON gb.GraderBatchID = bt.GraderBatchID
INNER JOIN
	sw_SeasonT AS st
ON st.SeasonID = gb.SeasonID
WHERE bt.GraderBatchID IS NOT NULL
GROUP BY SeasonDesc, bt.GraderBatchID, BinDeliveryID
ORDER BY SeasonDesc
