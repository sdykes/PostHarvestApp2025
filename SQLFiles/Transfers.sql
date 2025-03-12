SELECT 
	bt.BinDeliveryID,
	bd.BinDeliveryNo AS [Bin Delivery No],
	GraderBatchID,
	TransferDate,
	ctf.CompanyName AS [From storage site],
	ctt.CompanyName AS [To storage site]
FROM ma_BinT AS bt
INNER JOIN
	ma_Transfer_BinT tbt
ON tbt.BinID = bt.BinID
INNER JOIN
	ma_TransferT AS tt
ON tt.TransferID = tbt.TransferID
INNER JOIN
	ma_Bin_DeliveryT AS bd
ON bd.BinDeliveryID = bt.BinDeliveryID
INNER JOIN
	(
	SELECT
		CompanyID,
		CompanyName
	FROM sw_CompanyT 
	) AS ctf
ON ctf.CompanyID = tt.FromStorageSiteCompanyID
INNER JOIN
	(
	SELECT
		CompanyID,
		CompanyName
	FROM sw_CompanyT 
	) AS ctt
ON ctt.CompanyID = tt.ToStorageSiteCompanyID
