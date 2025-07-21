library(tidyverse)

con <- DBI::dbConnect(odbc::odbc(),    
                      Driver = "ODBC Driver 18 for SQL Server", 
                      Server = "abcrepldb.database.windows.net",  
                      Database = "ABCPackerRepl",   
                      UID = "abcadmin",   
                      PWD = "Trauts2018!",
                      Port = 1433
)

DefectAssessment <- DBI::dbGetQuery(con,
                                    "WITH defAss (Season,RPIN,Orchard,[Production site],[Management area],GraderBatchID,Defect,DefectQty)
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
                                    ON sampq.GraderBatchID = da.GraderBatchID")

GraderBatch <- DBI::dbGetQuery(con,
                               "SELECT 
	                                  gb.GraderBatchID,
	                                  GraderBatchNo AS [Grader Batch],
	                                  SeasonDesc AS Season,
	                                  Grower,
	                                  FarmCode AS RPIN,
	                                  FarmName AS Orchard,
	                                  SubdivisionCode AS [Production site],
	                                  HarvestDate AS [Harvest date],
	                                  PackDate AS [Pack date],
	                                  bu.[Bins tipped],  
	                                  [Packing site],
	                                  InputKgs AS [Input kgs],
	                                  COALESCE(WasteOtherKgs,0) + COALESCE(JuiceKgs,0) + COALESCE(SampleKgs,0) AS [Reject kgs],
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
                                LEFT JOIN
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
		                                    SUM(BinQty) AS [Bins tipped]
	                                      FROM ma_Bin_UsageT
	                                      WHERE GraderBatchID IS NOT NULL
	                                      GROUP BY GraderBatchID
	                                  ) AS bu
                                ON bu.GraderBatchID = gb.GraderBatchID")

DBI::dbDisconnect(con)

DA2025TeArai <- DefectAssessment |>
  filter(Season == 2025) |>
  inner_join(GraderBatch |>
               filter(Season == 2025,
                      `Batch closed` == 1,
                      `Packing site` == "Te Ipu Packhouse (RO)") |>
               mutate(Packout = 1-`Reject kgs`/`Input kgs`) |>
               select(c(GraderBatchID,`Reject kgs`,`Input kgs`)),
             by = "GraderBatchID") |>
  filter(Orchard == "Te Arai") |>
  group_by(`Production site`, Defect) |>
  summarise(DefectQty = sum(DefectQty),
            SampleQty = max(SampleQty),
            `Reject kgs` = sum(`Reject kgs`),
            `Input kgs` = sum(`Input kgs`)) |>
  mutate(Packout = 1-`Reject kgs`/`Input kgs`,
         defProp = (1-Packout)*DefectQty/SampleQty,
         defPerc = scales::percent(defProp, 0.1)) 

Top15 <- DA2025TeArai |>
  ungroup() |>
  group_by(Defect) |>
  summarise(DefectQty = sum(DefectQty),
            SampleQty = max(SampleQty),
            `Reject kgs` = sum(`Reject kgs`),
            `Input kgs` = sum(`Input kgs`)) |>
  mutate(Packout = 1-`Reject kgs`/`Input kgs`,
         defProp = (1-Packout)*DefectQty/SampleQty,
         defPerc = scales::percent(defProp, 0.1)) |>
  arrange(defProp) |>
  slice_tail(n=20) |>
  pull(Defect)



DA2025TeArai |>
  filter(Defect %in% Top15) |>
  mutate(Defect = factor(Defect, levels = Top15)) |>
  ggplot(aes(Defect, defProp)) +
  geom_col() +
  facet_wrap(~`Production site`) +
  coord_flip() +
  geom_text(aes(label = defPerc, y = defProp), size = 3.0, hjust = -0.2,
            position = position_dodge(width=0.9), colour = "black") +
  scale_y_continuous("Defect proportion / %", labels = scales::label_percent(0.1)) +
  scale_fill_manual(values = c("#a9342c","#48762e","#526280","#f6c15f")) +
  scale_colour_manual(values = c("#a9342c","#48762e","#526280","#f6c15f")) +
  ggthemes::theme_economist() + 
  theme(legend.position = "top",
        axis.title.x = element_text(margin = margin(t = 10), size = 14),
        axis.title.y = element_text(margin = margin(r = 10), size = 14),
        axis.text.y = element_text(size = 14, hjust=1),
        axis.text.x = element_text(size = 14),
        plot.background = element_rect(fill = "#D7E4F1", colour = "#D7E4F1"),
        legend.text = element_text(size = 14),
        legend.title = element_text(size = 14),
        strip.text = element_text(margin = margin(b=10), size = 14))

ggsave("TeAraiDefectComparison.png", width = 10, height = 8)

