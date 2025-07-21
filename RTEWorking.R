library(tidyverse)

con <- DBI::dbConnect(odbc::odbc(),    
                      Driver = "ODBC Driver 18 for SQL Server", 
                      Server = "abcrepldb.database.windows.net",  
                      Database = "ABCPackerRepl",   
                      UID = "abcadmin",   
                      PWD = "Trauts2018!",
                      Port = 1433
)

PoolDefinition <- DBI::dbGetQuery(con,
                                  "SELECT 
	                                      DISTINCT pr.ProductID
	                                      ,pr.ProductDesc AS [Product description]
	                                      ,fpt.PoolDesc AS [Pool description]
                                    FROM sw_ProductT AS pr
                                    INNER JOIN
	                                      sw_Tube_TypeT AS ttt
                                    ON ttt.TubeTypeID = pr.TubeTypeID
                                    INNER JOIN
	                                      sw_Tube_DiameterT AS tdt
                                    ON tdt.TubeDiameterID = ttt.TubeDiameterID
                                    INNER JOIN
	                                      fi_PoolT AS fpt
                                    ON fpt.TubeDiameterID = tdt.TubeDiameterID
                                    INNER JOIN
	                                      sw_GradeT AS gt
                                    ON gt.GradeID = pr.GradeID
                                    WHERE JuiceFlag = 0
                                    AND SampleFlag = 0")

RTEsFromFieldBins <- DBI::dbGetQuery(con,
                                    "SELECT
	pd.GraderBatchID 
	,gb.GraderBatchNo
	,pd.PalletDetailID 
	,pd.ProductID 
	,COUNT(DISTINCT ca.CartonNo) AS NoOfUnits 
	,COUNT(DISTINCT ca.CartonNo) * pr.TubesPerCarton * ttt.RTEConversion AS RTEs 
	,COUNT(DISTINCT ca.CartonNo) * pr.NetFruitWeight AS Kgs 
	,se.SeasonDesc AS Season 
	,cog.CompanyName AS Grower
	,fa.FarmCode AS RPIN
	,fa.FarmName AS Orchard
	,sb.SubdivisionCode AS [Production site]
	,'FromFieldBins' AS RTESource
FROM  ma_PalletT AS pa 
INNER JOIN
	ma_Pallet_DetailT AS pd 
ON pd.PalletID = pa.PalletID 
INNER JOIN
	ma_Grader_BatchT AS gb 
ON gb.GraderBatchID = pd.GraderBatchID 
INNER JOIN
	sw_SeasonT AS se
ON se.SeasonID = pa.SeasonID
INNER JOIN
	sw_CompanyT AS cog
ON cog.CompanyID = gb.GrowerCompanyID
INNER JOIN
	sw_FarmT AS fa
ON fa.FarmID = gb.FarmID
INNER JOIN
	sw_SubdivisionT AS sb 
ON sb.SubdivisionID = gb.SubdivisionID 
INNER JOIN
	ma_CartonT AS ca 
ON ca.PalletDetailID = pd.PalletDetailID 
INNER JOIN
	sw_ProductT AS pr
ON pr.ProductID = pd.ProductID
INNER JOIN	
	sw_Tube_TypeT AS ttt
ON ttt.TubeTypeID = pr.TubeTypeID
INNER JOIN
	sw_GradeT AS gt
ON gt.GradeID = pr.GradeID
LEFT JOIN
	ma_Bin_DeliveryT AS bi 
ON bi.BinDeliveryID = ca.BinDeliveryID 
LEFT JOIN
	sw_Farm_BlockT AS fb 
ON fb.BlockID = bi.BlockID
WHERE gb.PresizeInputFlag = 0 
AND pr.SampleFlag = 0
AND ca.PalletDamageID IS NULL
GROUP BY 
	pd.GraderBatchID 
	,gb.GraderBatchNo
	,pd.PalletDetailID 
	,pd.ProductID 
	,ttt.RTEConversion 
	,pr.TubesPerCarton 
	,pr.NetFruitWeight 
	,se.SeasonDesc 
	,cog.CompanyName
	,fa.FarmCode 
	,fa.FarmName 
	,sb.SubdivisionCode 
UNION ALL
SELECT 
	pd.GraderBatchID 
	,gb.GraderBatchNo
	,pd.PalletDetailID 
	,pd.ProductID
	,pd.NoOfUnits AS NoOfUnits 
	,SUM(CAST((ebd.KGWeight / ttt.PresizeAvgTubeWeight) * ttt.RTEConversion AS numeric(10, 2))) AS RTEs 
	,SUM(ebd.KGWeight) AS Kgs 
	,se.SeasonDesc AS Season 
	,cog.CompanyName AS Grower
	,fa.FarmCode AS RPIN
	,fa.FarmName AS Orchard
	,sb.SubdivisionCode AS [Production site]
	,'FromFieldBins' AS RTESource
FROM  ma_PalletT AS pa 
INNER JOIN
      ma_Pallet_DetailT pd 
ON pd.PalletID = pa.PalletID 
INNER JOIN
      ma_Grader_BatchT gb 
ON gb.GraderBatchID = pd.GraderBatchID 
INNER JOIN
	sw_SeasonT AS se
ON se.SeasonID = pa.SeasonID
INNER JOIN
	sw_CompanyT AS cog
ON cog.CompanyID = gb.GrowerCompanyID
INNER JOIN
	sw_FarmT AS fa
ON fa.FarmID = gb.FarmID
INNER JOIN
      sw_SubdivisionT sb 
ON sb.SubdivisionID = gb.SubdivisionID 
INNER JOIN
       ma_Export_Bin_DetailT ebd 
ON ebd.PalletDetailID = pd.PalletDetailID 
INNER JOIN
	sw_ProductT AS pr
ON pr.ProductID = pd.ProductID
INNER JOIN
	sw_Tube_TypeT AS ttt
ON ttt.TubeTypeID = pr.TubeTypeID
WHERE gb.PresizeInputFlag = 0 
AND pr.SampleFlag = 0
GROUP BY 
	pd.GraderBatchID 
	,gb.GraderBatchNo
	,pd.PalletDetailID 
	,pd.ProductID 
	,pd.NoOfUnits 
	,se.SeasonDesc
	,cog.CompanyName
	,fa.FarmCode 
	,fa.FarmName 
	,sb.SubdivisionCode 
")

RTEsFromPresize <- DBI::dbGetQuery(con,
                                  "SELECT 
	bd.PresizeOutputFromGraderBatchID AS GraderBatchID 
	,gb.GraderBatchNo
	,NULL AS PalletDetailID
	,bd.PresizeProductID AS ProductID
	,NULL AS NoOfUnits
	,CAST(ROUND(bd.TotalWeight / ttt.PresizeAvgTubeWeight, 0) * ttt.RTEConversion AS numeric(10, 2)) AS RTEs 
	,bd.TotalWeight AS Kgs 
	,se.SeasonDesc AS Season 
	,cog.CompanyName AS Grower
	,fa.FarmCode AS RPIN
	,fa.FarmName AS Orchard
	,sb.SubdivisionCode AS [Production site] 
	,'Presize' AS RTESource
FROM  ma_Bin_DeliveryT AS bd 
INNER JOIN
	ma_Grader_BatchT AS gb 
ON gb.GraderBatchID = bd.PresizeOutputFromGraderBatchID 
INNER JOIN
	sw_ProductT AS pr 
ON pr.ProductID = bd.PresizeProductID
INNER JOIN 
	sw_Tube_TypeT AS ttt
ON ttt.TubeTypeID = pr.TubeTypeID
INNER JOIN
	sw_GradeT AS gt
ON gt.GradeID = pr.GradeID
INNER JOIN
	sw_SeasonT AS se
ON se.SeasonID = gb.SeasonID
INNER JOIN
	sw_CompanyT AS cog
ON cog.CompanyID = gb.GrowerCompanyID
INNER JOIN
	sw_FarmT AS fa
ON fa.FarmID = gb.FarmID
INNER JOIN
    sw_SubdivisionT AS sb 
ON sb.SubdivisionID = gb.SubdivisionID
WHERE bd.PresizeFlag = 1 
AND gt.JuiceFlag = 0")

RTEsFromRepack <- DBI::dbGetQuery(con,
                                  "SELECT 
	re.GraderBatchID 
	,gb.GraderBatchNo
	,pd.PalletDetailID 
	,pd.ProductID
	,SUM(cam.NoOfUnits) AS NoOfUnits 
	,SUM(cam.RTEs) AS RTEs
	,SUM(cam.Kgs) AS Kgs
	,se.SeasonDesc AS Season 
	,cog.CompanyName AS Grower
	,fa.FarmCode AS RPIN
	,fa.FarmName AS Orchard
	,sb.SubdivisionCode AS [Production site]
	,'Repack' AS RTESource
FROM  dbo.ma_RepackT AS re 
INNER JOIN
   ma_Repack_Input_CartonT AS ca 
ON ca.RepackID = re.RepackID 
INNER JOIN
   (
   SELECT 
		ric.RepackInputCartonID, 
		CAST(CASE 
				WHEN ric.FromExportBinDetailID IS NULL THEN 1 
				ELSE 0 
			END AS numeric(10, 2)) AS NoOfUnits, 
		CAST(CASE 
				WHEN ric.FromExportBinDetailID IS NULL THEN pr.TubesPerCarton * ttt.RTEConversion 
				ELSE (ebd.KGWeight / ttt.PresizeAvgTubeWeight) * ttt.RTEConversion 
			END AS numeric(10, 2)) AS RTEs, 
		CASE 
			WHEN ric.FromExportBinDetailID IS NULL THEN pr.NetFruitWeight 
			ELSE ebd.KGWeight 
		END AS Kgs
	FROM  dbo.ma_Repack_Input_CartonT AS ric 
	INNER JOIN
		ma_Pallet_DetailT AS pd 
	ON pd.PalletDetailID = ric.FromPalletDetailID 
	INNER JOIN
		sw_ProductT AS pr
	ON pr.ProductID = pd.ProductID
	INNER JOIN 
		sw_Tube_TypeT AS ttt
	ON ttt.TubeTypeID = pr.TubeTypeID
	LEFT JOIN
		ma_Export_Bin_DetailT AS ebd 
	ON ebd.ExportBinDetailID = ric.FromExportBinDetailID 
	LEFT JOIN
		(
		SELECT 
			ExportBinID, 
			SUM(KGWeight) AS TotalExportBinWeight
		FROM ma_Export_Bin_DetailT
		GROUP BY ExportBinID
		) AS eto 
	ON eto.ExportBinID = ebd.ExportBinID
	) AS cam
ON cam.RepackInputCartonID = ca.RepackInputCartonID 
INNER JOIN
   ma_Pallet_DetailT AS pd 
ON pd.PalletDetailID = ca.FromPalletDetailID 
INNER JOIN
   ma_Grader_BatchT AS gb 
ON gb.GraderBatchID = re.GraderBatchID 
INNER JOIN
	sw_CompanyT AS cog
ON cog.CompanyID = gb.GrowerCompanyID
INNER JOIN
	sw_FarmT AS fa
ON fa.FarmID = gb.FarmID
INNER JOIN
   sw_SubdivisionT AS sb 
ON sb.SubdivisionID = gb.SubdivisionID
INNER JOIN
	sw_SeasonT AS se
ON se.SeasonID = re.SeasonID
WHERE (gb.PresizeInputFlag = 0)
GROUP BY 
	re.GraderBatchID
	,gb.GraderBatchNo
	,cog.CompanyName
	,fa.FarmCode 
	,fa.FarmName 
	,sb.SubdivisionCode 
	,pd.PalletDetailID
	,pd.ProductID
	,se.SeasonDesc")

BinsTipped <- DBI::dbGetQuery(con,
                              "SELECT 
	GraderBatchID
	,SUM(BinQty) AS BinQty
FROM ma_Bin_UsageT
WHERE GraderBatchID IS NOT NULL
GROUP BY GraderBatchID")

DBI::dbDisconnect(con)

PDMod <- PoolDefinition |>
  mutate(Pool = str_sub(`Pool description`, 15, -1),
         `Pool description` = if_else(Pool %in% c(0,140,145,160,165),
                                      "Mixed",
                                      as.character(Pool))) |>
  filter(Pool != 52)

RTEs <- RTEsFromFieldBins |>
  bind_rows(RTEsFromRepack) |>
  bind_rows(RTEsFromPresize)

RTEsFFBWithPools <- RTEs |>
  filter(Season == 2025) |>
  left_join(PDMod |> select(c(ProductID,`Pool description`)),
            by = "ProductID")

RTEByGB <- RTEsFFBWithPools |>
  group_by(GraderBatchID, GraderBatchNo,Grower,RPIN,Orchard,`Production site`,`Pool description`) |>
  summarise(RTEs = sum(RTEs, na.rm=T),
            .groups = "drop") |>
  pivot_wider(id_cols = c(GraderBatchID, GraderBatchNo,Grower,RPIN,Orchard,`Production site`),
              names_from = `Pool description`,
              values_from = RTEs,
              values_fill = 0) |>
  rowwise() |>
  mutate(TotalRTEs = sum(c_across(`58`:Mixed))) |>
  left_join(BinsTipped, by = "GraderBatchID") |>
  mutate(RTEsPerBin = TotalRTEs/BinQty) |>
  filter(!is.na(BinQty))

if (RTEAgg == "Batch") {

  RTEByGB
  
} else if (RTEAgg == "Production site") {
  
  RTEByPS <- RTEByGB |>
    group_by(Grower,RPIN,Orchard,`Production site`) |>
    summarise(across(.cols = c(`58`:BinQty), ~sum(.,na.rm=T)),
              .groups = "drop") |>
    mutate(RTEsPerBin = TotalRTEs/BinQty)
  
  RTEByPS
  
} else if (RTEAgg == "Orchard/RPIN") {

  RTEByRPIN <- RTEByGB |>
    filter(!is.na(BinQty)) |>
    group_by(Grower,RPIN,Orchard) |>
    summarise(across(.cols = c(`58`:BinQty), ~sum(.,na.rm=T)),
              .groups = "drop") |>
    mutate(RTEsPerBin = TotalRTEs/BinQty)
  
  RTEByRPIN
  
} else {
  
  RTEByGrower <- RTEByGB |>
    group_by(Grower) |>
    summarise(across(.cols = c(`58`:BinQty), ~sum(.,na.rm=T)),
              .groups = "drop") |>
    mutate(RTEsPerBin = TotalRTEs/BinQty)
  
  RTEByGrower

}



  

