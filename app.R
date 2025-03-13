
library(shiny)
library(shinydashboard)
library(tidyverse)
library(shinyjs)
library(shinyauthr)
library(sodium)

################################################################################
#                               Grower List                                    #
################################################################################

# Import the userdb and the permission list

password_lookup <- read_csv("userdb.csv", show_col_types = FALSE) |>
  mutate(Permissions = str_replace_all(Permissions, fixed(" "), "")) 

# dataframe that holds usernames, passwords and other user data

user_base <- tibble(
  user = password_lookup$Username,
  password = password_lookup$Password, 
  password_hash = sapply(password_lookup$Password, sodium::password_store), 
  permissions = password_lookup$Permissions,
  name = password_lookup$User
)

#=================================SQL function==================================

con <- DBI::dbConnect(odbc::odbc(),    
                      Driver = "SQLServer", #"ODBC Driver 18 for SQL Server", #
                      Server = "abcrepldb.database.windows.net",  
                      Database = "ABCPackerRepl",   
                      UID = "abcadmin",   
                      PWD = "Trauts2018!",
                      Port = 1433
)

BinDelivery <- DBI::dbGetQuery(con, 
                               "SELECT 
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
	                                    COALESCE([Bins in process],0) AS [Bins in process],
	                                    NoOfBins - COALESCE([Bins in process],0) AS [Bins currently in storage],
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
		                                      SUM(BinQty) AS [Bins in process]
	                                    FROM ma_Bin_UsageT AS bu
	                                    WHERE bu.GraderBatchID IS NOT NULL
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
                                  WHERE PresizeFlag = 0")

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

PhytoAss <- DBI::dbGetQuery(con,
                            "SELECT 
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
                            AND TemplateID = 10")

MPILots <- DBI::dbGetQuery(con,
                           "SELECT 
	                              SeasonDesc AS Season,
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
                            ON st.SeasonID = gb.SeasonID")

BinUsage <- DBI::dbGetQuery(con,
                            "WITH BinsTipped_CTE (BinDeliveryID, BinQty, GraderBatchID, BinsTipped)
                            AS
                            (	
                            SELECT 
                                BinDeliveryID,
                                BinQty,
                                bu.GraderBatchID,
                                CASE
                                  WHEN ClosedDateTime IS NOT NULL THEN 'Tipped'
                                  ELSE 'In process'
                                END AS BinsTipped
                            FROM ma_Bin_UsageT AS bu
                            INNER JOIN
                                ma_Grader_batchT AS gb
                            ON gb.GraderBatchID = bu.GraderBatchID
                            WHERE bu.GraderBatchID IS NOT NULL
                            )
                            SELECT 
                                BinDeliveryID,
                                BinsTipped,
                                SUM(BinQty) AS BinQty
                            FROM BinsTipped_CTE
                            GROUP BY BinDeliveryID, BinsTipped")

GrowerOrchard <- DBI::dbGetQuery(con,
                                 "SELECT 
                                      FarmName AS Orchard,
                                      CompanyName AS Grower
                                  FROM sw_FarmT AS ft
                                  INNER JOIN
                                      sw_CompanyT AS ct
                                  ON ct.CompanyID = ft.GrowerCompanyID
                                  WHERE ft.ActiveFlag = 1")


DBI::dbDisconnect(con)

#Generate the permissionsList

Growers <- GrowerOrchard |>
  distinct(Grower) |>
  pull()

GOList <- function(Grower) {
  list(Growers = c({{Grower}}),
       Orchards = GrowerOrchard |> 
         filter(Grower == {{Grower}}) |>
         pull(Orchard))
}

GrowerOrchardList <- Growers |>
  map(~GOList(.))

# Need to collapse all of the whitespaces between the words

names(GrowerOrchardList) <- tibble(Growers = Growers) |>
  mutate(Growers = str_replace_all(Growers, fixed(" "), "")) |>
  pull(Growers)

## Specialty Permissions

Havelock <- list(Growers = c("ROLP 1", "Rakete"),
                 Orchards=c("Stock Roads","Home Block","Manahi","Te Aute Road North","Te Aute Road South","Raukawa","Lobb"))
Hastings <- list(Growers = c("ROLP 1", "ROLP 2", "Heretaunga Orchards Limited Partnership","Lawn Road Orchard Limited"),
                 Orchards = c("Napier Road North","Napier Road Central","Napier Road South","Haumoana","Ormond Road","Lawn Road"))
Maraekakaho <- list(Growers = c("Mana Orchards Limited Partnership","Pioneer Capital Molly Limited","Rockit Orchards Limited"),
                    Orchards = c("Mana1","Mana2","Pioneer Orchard","Valley Road"))
Crownthorpe <- list(Growers = c("Rockit Orchards Limited","Heretaunga Orchards Limited Partnership","Rakete","ROLP 1","ROLP 2"),
                    Orchards = c("Crown","Lowry Heretaunga","Lowry","Rangi2","Sim1","Sim2","Steel","Wharerangi Orchard","Omahu"))
RMS <- list(Growers = c("ROLP 1","ROLP 2","Rakete","Heretaunga Orchards Limited Partnership","Pioneer Capital Molly Limited","Rockit Orchards Limited"),
            Orchards = c("Home Block","Raukawa","Te Aute Road North","Wharerangi Orchard","Stock Roads","Te Aute Road South",
                         "Napier Road South","Omahu","Haumoana","Napier Road Central","Napier Road North","Rangi2","Lobb","Sim1",
                         "Steel","Manahi","Sim2","Crown","Ormond Rd","Lowry Heretaunga","Pioneer Orchard","Lowry","Valley Road"))
RaketePlus <- list(Growers = c("Rakete","Heretaunga Orchards Limited Partnership","Te Arai Orchard Limited Partnership"),
                   Orchards = c("Sim1","Sim2","Steel","Manahi","Lobb","Ormond","Crown","Lowry Heretaunga","Te Arai"))
Goodwin <- list(Growers = c("Mana Orchards Limited Partnership","Lawn Road Orchard Limited"),
                Orchards = c("Mana1","Mana2","Lawn Road"))
Craigmore <- list(Growers = c("Springhill Horticulture Limited","Waipaoa Horticulture Limited"),
                  Orchards = c("Springhill East","Springhill West","Sunpark","Kahahakuri"))
Zame <- list(Growers = c("Rockit Longacre","Rockit Watson Road Partnership"),
             Orchards = c("Watson Road","Longacre Orchard"))
ZamePlus <- list(Growers = c("Rockit Longacre","Rockit Watson Road Partnership","ROLP 2"),
                 Orchards = c("Watson Road","Longacre Orchard","Napier Road South","Omahu","Haumoana","Napier Road Central","Napier Road North","Rangi2"))
AgFirstPlus <- list(Growers = c("AgFirst Engineering Gisborne","Howatson Rural Holdings Limited","Te Arai Orchard Limited Partnership"),
                    Orchards = c("Karaua","Te Arai","Matarangi"))
Longzana <- list(Growers = c("Longlands Orchard Limited Partnership","Manzana Orchard Limited Partnership"),
                 Orchards = c("Manzana 1","Manzana 2","Longlands")) 
XfruitPlus <- list(Growers = c("X Fruit Limited","Mangapoike Family Trust"),
                   Orchards = c("Norton","Paki Paki","Sissons","Parkhill Orchard","Mangapoike"))
Macleod <- list(Grower = c("ROLP 1","Longlands Orchard Limited Partnership","Manzana Orchard Limited Partnership"),
                Orchards = c("Home Block","Raukawa","Te Aute Road North","Wharerangi Orchard","Stock Roads","Te Aute Road South",
                             "Manzana 1","Manzana 2","Longlands"))
Punchbowl <- list(Grower = c("ROLP 1","ROLP 2","Longlands Orchard Limited Partnership"),
                  Orchards = c("Home Block","Raukawa","Te Aute Road North","Wharerangi Orchard","Stock Roads","Te Aute Road South",
                               "Napier Road South","Omahu","Haumoana","Napier Road Central","Napier Road North","Rangi2","Longlands"))


SpecialtyPermissions <- list(Havelock=Havelock,
                             Hastings = Hastings,
                             Maraekakaho = Maraekakaho,
                             Crownthorpe = Crownthorpe,
                             RMS = RMS,
                             RaketePlus=RaketePlus, 
                             Goodwin=Goodwin, 
                             Craigmore=Craigmore,
                             Zame=Zame,
                             ZamePlus=ZamePlus,
                             AgFirstPlus=AgFirstPlus,
                             Longzana=Longzana,
                             XfruitPlus=XfruitPlus,
                             Macleod=Macleod,
                             Punchbowl=Punchbowl)

permissions <- c(GrowerOrchardList, SpecialtyPermissions)

box_height = "50em"
plot_height = "46em"

ui <- dashboardPage(

# Define header part of the dashboard
  dashboardHeader(
    title = tags$img(src="Rockit2.png", width="100"),
    titleWidth = 300
    #tags$li(class = "dropdown", 
    #        style = "padding: 8px; color: #a9342c;",
    #        shinyauthr::logoutUI("logout")),
    #tags$img(src="Rockit2.png", width="200")
    ),

## Sidebar content
  dashboardSidebar(
    width = 300,
    collapsed = TRUE, 
    minified = F,
    selectInput(inputId = "Grower", 
                label = "Grower", 
                choices = unique(BinDelivery$Grower)),
    checkboxGroupInput(inputId = "Orchards",
                       label = h5("Select one or more orchards:"),
                       choices = '')
  ),

## Body content
  dashboardBody(
    shinyjs::useShinyjs(),
    tags$head(tags$style(".table{margin: 0 auto;}"),
              tags$script(src="https://cdnjs.cloudflare.com/ajax/libs/iframe-resizer/3.5.16/iframeResizer.contentWindow.min.js",
                          type="text/javascript"),
              includeScript("returnClick.js"),
              tags$link(rel = "stylesheet", type="text/css", href="custom.css")
    ),
    shinyauthr::loginUI("login"),
    tabsetPanel(type="tabs",
                tabPanel("Bin Accounting",
                         fluidRow(
                           box(title = "Bin summary by orchard",width = 12, 
                               DT::dataTableOutput("BinAccountingRPIN")),
                           box(title = "Bin summary by poduction site",width = 12, 
                               DT::dataTableOutput("BinAccountingProdSite"))
                         )
                ),
                tabPanel("Bins received",
                         fluidRow(
                           box(title = "Detailed consignment listing",width = 12, 
                               DT::dataTableOutput("ConsignmentDetail")),
                           downloadButton("downloadReceived", "download table", class = "butt1")
                         )
                ),
                tabPanel("Bin storage by location and type",
                         fluidRow(
                           box(title = "Bin storage by RPIN",width = 12, 
                               DT::dataTableOutput("StorageByRPIN"))
                         )
                ),
                tabPanel("Bins tipped",
                         fluidRow(
                           box(title = "Closed batches - Te Ipu",width = 12, 
                               DT::dataTableOutput("ClosedBatchesTeIpu")),
                           box(title = "Closed batches - Sunfruit",width = 12, 
                               tags$p(strong("The packouts stated in this table are preliminary and subject to normalisation and potential change"),
                                      style = "color: #a9342c"),
                               DT::dataTableOutput("ClosedBatchesSF")),
                           box(title = "Closed batches - Kiwi Crunch",width = 12, 
                               tags$p(strong("The packouts stated in this table are preliminary and subject to normalisation and potential change"),
                                      style = "color: #a9342c"),
                               DT::dataTableOutput("ClosedBatchesKC")),
                           box(title = "Open batches by production site",width=12,
                               DT::dataTableOutput("OpenBatchesPS")),
                           downloadButton("closedBatches", "download table", class = "butt1")
                         )
                ),
                tabPanel("Packout plots",
                         fluidRow(
                           tags$h2(strong("\t Te Ipu packouts"),style = "color: #a9342c"),
                           box(width=4, selectInput("dateInput", "select the required x-axis", 
                                                    c("Storage days", "Pack date", "Harvest date")))
                         ),
                         fluidRow(
                           box(width=12, plotOutput("packoutPlotTeIpu"))
                         ),
                         fluidRow(
                           tags$h2(strong("\t Sunfruit packouts"),style = "color: #a9342c"),
                           tags$p("\t Note - Sunfruit packouts can only be viewed as a function of pack date"),
                           tags$p(strong("The packouts stated in this plot are preliminary and subject to normalisation and potential change"),
                                  style = "color: #a9342c"),
                           box(width=12, plotOutput("packoutPlotSF"))
                         )
                ),
                tabPanel("Defect plots",
                         fluidRow(
                           box(width = 12, height = box_height, plotOutput("defectPlot", height = plot_height))
                         ),
                         fluidRow(
                           box(width = 12, plotOutput("defectHeatMap")),
                         ),
                         downloadButton(
                           "downloadDefect", "download table"
                         )
                ),
                tabPanel("Phytosanitary tracking",
                         fluidRow(
                           column(title = "Proportion of Excluded MPI lots",width = 6, 
                             DT::dataTableOutput("ExcludedMPILots")),
                           column(title = "Pest interceptions by type",
                               plotOutput("PestInterceptions"), width=6)
                        )
                )
    )
  )
)


# Define server logic 
server <- function(input, output, session) {
  
# call login module supplying data frame, user and password cols
# and reactive trigger
  credentials <- shinyauthr::loginServer( 
    id = "login", 
    data = user_base,
    user_col = user,
    pwd_col = password_hash,
    sodium_hashed = TRUE,
    log_out = reactive(logout_init())
    )
  
  # call the logout module with reactive trigger to hide/show
    logout_init <- shinyauthr::logoutServer(
      id = "logout", 
      active = reactive(credentials()$user_auth))
  
# un-collapse the sidebar after login
  
  observe({
    if(credentials()$user_auth) {
      shinyjs::removeClass(selector = "body", class = "sidebar-collapse")
    } else {
      shinyjs::addClass(selector = "body", class = "sidebar-collapse")
    }
  })
  
## This code defines the orchards for selection
  
  user_info <- reactive({
    credentials()$info
  })
  
  orchardOwner <- reactive({
    if(credentials()$user_auth) {
      if(user_info()$permissions == "admin") {
        BinDelivery |>
          filter(Grower == input$Grower) 
      } else {
        BinDelivery |>
          filter(Grower %in% eval(parse(text = str_c("permissions$",user_info()$permissions,"$Growers"))))
      } 
    }
  })
  
  
  observeEvent(orchardOwner(), {
    if(credentials()$user_auth) {
      if(user_info()$permissions == "admin") {
        updateCheckboxGroupInput(session = session,
                                 inputId = "Orchards",
                                 choices = unique(orchardOwner()$Orchard),
                                 selected = orchardOwner()$Orchard[1])
      } else {
        updateSelectInput(session = session, 
                          inputId = "Grower",
                          choices = unique(orchardOwner()$Grower))
        updateCheckboxGroupInput(session = session,
                                 inputId = "Orchards",
                                 choices = choices <- eval(parse(text = str_c("permissions$",user_info()$permissions,"$Orchards"))),
                                 selected = orchardOwner()$Orchard[1])
      }
    }
  })

## # Bin accounting tab....consignment by RPIN 
  
# Determine bins tipped and Bins in process from Bin Usage dataframe:
  
  BinDeliveryFull <- BinDelivery |>
    left_join(BinUsage |>
                pivot_wider(id_cols = BinDeliveryID,
                            names_from = BinsTipped,
                            values_from = BinQty,
                            values_fill = 0),
              by = "BinDeliveryID") |>
    mutate(across(.cols = c(`In process`,Tipped), ~replace_na(.,0))) |>
    select(-c(`Bins in process`))
  
  output$BinAccountingRPIN <- DT::renderDataTable({
    #req(credentials()$user_auth)

# Total for the bottom of the table
    
    RPINTotal <- BinDeliveryFull |>
      filter(Orchard %in% input$Orchards,
             Season == 2025) |>
      select(c(RPIN, Orchard, `Bins received`,`In process`,Tipped,`Bins currently in storage`)) |>
      summarise(`Received` = sum(`Bins received`),
                `In process` = sum(`In process`),
                Tipped = sum(Tipped),
                `Currently in storage` = sum(`Bins currently in storage`)) |>
      mutate(RPIN = "Total",
             Orchard = "") |>
      relocate(RPIN, .before = `Received`) |>
      relocate(Orchard, .after = RPIN)
    
  # Define the table itself
    
    DT::datatable(BinDeliveryFull |>
                    filter(Orchard %in% input$Orchards,
                           Season == 2025) |>
                    select(c(RPIN, Orchard, `Bins received`,`In process`,Tipped,`Bins currently in storage`)) |>
                    group_by(RPIN, Orchard) |>
                    summarise(`Received` = sum(`Bins received`),
                              `In process` = sum(`In process`),
                              Tipped = sum(Tipped),
                              `Currently in storage` = sum(`Bins currently in storage`),
                              .groups = "drop") |>
                    bind_rows(RPINTotal),
                  options = list(scrollX = TRUE),
                  escape = FALSE,
                  rownames = FALSE)
    
  })
  
# Bin accounting tab....consignment by production site  
  
  output$BinAccountingProdSite <- DT::renderDataTable({  
    #req(credentials()$user_auth)
    
# Total for the bottom of the table
    
    PSTotal <- BinDeliveryFull |>
      filter(Orchard %in% input$Orchards, #c("Home Block"), #
             Season == 2025) |>
      select(c(RPIN, Orchard, `Production site`, `Bins received`,`In process`,Tipped,`Bins currently in storage`)) |>
      summarise(`Received` = sum(`Bins received`),
                `In process` = sum(`In process`),
                Tipped = sum(Tipped),
                `Currently in storage` = sum(`Bins currently in storage`)) |>
      mutate(RPIN = "Total",
             Orchard = "",
             `Production site` = "") |>
      relocate(RPIN, .before = `Received`) |>
      relocate(Orchard, .after = RPIN) |>
      relocate(`Production site`, .after = Orchard)
    
  
    DT::datatable(BinDeliveryFull |>
                    filter(Orchard %in% input$Orchards, #c("Home Block"), #
                    Season == 2025) |>
                    select(c(RPIN, Orchard, `Production site`,`Bins received`,`In process`,Tipped,`Bins currently in storage`)) |>
                    group_by(RPIN,Orchard,`Production site`) |>
                    summarise(`Received` = sum(`Bins received`),
                              `In process` = sum(`In process`),
                              Tipped = sum(Tipped),
                              `Currently in storage` = sum(`Bins currently in storage`),
                              .groups = "drop") |>
                    bind_rows(PSTotal),
                  options = list(scrollX = TRUE),
                  escape = FALSE,
                  rownames = FALSE)

  })
  
 # Bins received tab....detailed consignment listing
  
  output$ConsignmentDetail <- DT::renderDataTable({  
    #req(credentials()$user_auth)
    
    DT::datatable(BinDelivery |>
                    filter(Orchard %in% input$Orchards,
                           Season == 2025) |>
                    select(-c(BinDeliveryID, `Bins in process`)) ,
                  options = list(scrollX = TRUE),
                  escape = FALSE,
                  rownames = FALSE)
    
  })
  
##################################################################################
#                            Down load button                                    #
############################# Bins received #####################################
  
  receievedTable <- reactive({ 
    #req(credentials()$user_auth)
    BinDelivery |>
      filter(Orchard %in% input$Orchards,
             Season == 2025) |>
      dplyr::select(c(`Bin delivery No`,RPIN,Orchard,`Production site`,`Management area`,
                      `Harvest Date`,`Bins received`,`Bins in process`,
                      `Bins currently in storage`,`Submission profile`,`Pick No`,
                      `Storage site`,`Storage type`,SmartFreshed)) 
    
  })  
  
  
  output$downloadReceived <- downloadHandler(
    filename = function() {
      paste0("binsReceived-",Sys.Date(),".csv")
    },
    content = function(file) {
      write.csv(receievedTable(), file)
    }
  )
  
# Bin storage by RPIN
  
  output$StorageByRPIN <- DT::renderDataTable({
    #req(credentials()$user_auth)
    
    StorageRPINTotal <- BinDelivery |>
      filter(Orchard %in% input$Orchards, #c("Home Block"), #
             Season == 2025) |>
      select(c(RPIN, Orchard, `Bins received`,`Bins currently in storage`)) |>
      summarise(`Bins received` = sum(`Bins received`),
                `Bins currently in storage` = sum(`Bins currently in storage`)) |>
      mutate(RPIN = "Total",
             Orchard = "",
             `Storage site` = "",
             `Storage type` = "") |>
      relocate(`Bins received`, .after = `Storage type`) |>
      relocate(`Bins currently in storage`, .after = `Bins received`)
    
 # Define the table itself
    
    DT::datatable(BinDelivery |>
                    filter(Orchard %in% input$Orchards, #c("Home Block"), #
                           Season == 2025) |>
                    select(c(RPIN, Orchard, `Storage site`,`Storage type`,`Bins received`,`Bins currently in storage`)) |>
                    group_by(RPIN, Orchard, `Storage site`,`Storage type`) |>
                    summarise(`Bins received` = sum(`Bins received`),
                              `Bins currently in storage` = sum(`Bins currently in storage`),
                              .groups = "drop") |>
                    pivot_wider(id_cols = c(RPIN, Orchard, `Storage site`),
                                names_from = `Storage type`,
                                values_from = c(`Bins received`,`Bins currently in storage`),
                                values_fill = 0),
                  options = list(scrollX = TRUE),
                  escape = FALSE,
                  rownames = FALSE)
    
  })
  

# bin tipped tab - Closed batches Te Ipu
  
  output$ClosedBatchesTeIpu <- DT::renderDataTable({  
    #req(credentials()$user_auth)
    
    GBTeIpuSummary <- GraderBatch |>
      filter(Season == 2025,
             Orchard %in% input$Orchards,
             `Packing site` == "Te Ipu Packhouse (RO)",
             `Batch closed` == 1) |>
      mutate(Packout = 1-`Reject kgs`/`Input kgs`)
    
    DT:: datatable(GBTeIpuSummary |>
                     select(-c(GraderBatchID,Grower,`Packing site`,`Batch closed`)) |>
                     mutate(across(.cols = c(`Input kgs`,`Reject kgs`), ~scales::comma(.,1.0)),
                            Packout = scales::percent(Packout, 0.1)),
                   options = list(scrollX = TRUE),
                   escape = FALSE,
                   rownames = FALSE)
    
  })
  
# bins tipped tabs - closed batches Sunfruit
  
  output$ClosedBatchesSF <- DT::renderDataTable({  
    #req(credentials()$user_auth)
    
    GBSFSummary <- GraderBatch |>
      filter(Season == 2025,
             Orchard %in% input$Orchards,
             `Packing site` == "Sunfruit Limited",
             `Batch closed` == 1) |>
      mutate(Packout = 1-`Reject kgs`/`Input kgs`)
    
    DT:: datatable(GBSFSummary |>
                     select(-c(GraderBatchID,Grower,`Packing site`,`Batch closed`)) |>
                     mutate(across(.cols = c(`Input kgs`,`Reject kgs`), ~scales::comma(.,1.0)),
                            Packout = scales::percent(Packout, 0.1)),
                   options = list(scrollX = TRUE),
                   escape = FALSE,
                   rownames = FALSE)
    
  })
  
# bins tipped tabs - closed batches Kiwi Crunch
  
  output$ClosedBatchesKC <- DT::renderDataTable({  
    #req(credentials()$user_auth)
    
    GBKCSummary <- GraderBatch |>
      filter(Season == 2025,
             Orchard %in% input$Orchards,
             `Packing site` == "Kiwi crunch (FV)",
             `Batch closed` == 1) |>
      mutate(Packout = 1-`Reject kgs`/`Input kgs`)
    
    DT:: datatable(GBKCSummary |>
                     select(-c(GraderBatchID,Grower,`Packing site`,`Batch closed`)) |>
                     mutate(across(.cols = c(`Input kgs`,`Reject kgs`), ~scales::comma(.,1.0)),
                            Packout = scales::percent(Packout, 0.1)),
                   options = list(scrollX = TRUE),
                   escape = FALSE,
                   rownames = FALSE)
    
  })
  
# bins tipped tabs - bins associated with open batches by PS
  
  output$OpenBatchesPS <- DT::renderDataTable({  
    #req(credentials()$user_auth)
    
    OpenBatchesPSTotal <- GraderBatch |>
      filter(Season == 2025,
             Orchard %in% input$Orchards, #("Home Block"),#
             #`Packing site` == "Te Ipu Packhouse (RO)",
             `Batch closed` == 0) |>
      select(-c(GraderBatchID,Season,Grower,`Batch closed`)) |>
      summarise(`Bins to be tipped` = sum(`Bins tipped`, na.rm=T)) |>
      mutate(RPIN = "Total",
             Orchard = "",
             `Production site` = "",
             `Packing site` = "") |>
      select(c(RPIN, Orchard, `Production site`,`Packing site`,`Bins to be tipped`))
 
    DT:: datatable(GraderBatch |>
                     filter(Season == 2025,
                            Orchard %in% input$Orchards, #c("Home Block"),#
                            #`Packing site` == "Te Ipu Packhouse (RO)",
                            `Batch closed` == 0) |>
                     select(-c(GraderBatchID,Grower,`Batch closed`)) |>
                     group_by(RPIN,Orchard,`Production site`,`Packing site`) |>
                     summarise(`Bins to be tipped` = sum(`Bins tipped`, na.rm=T),
                               .groups = "drop") |>
                     bind_rows(OpenBatchesPSTotal) |>
                     mutate(`Bins to be tipped` = scales::comma(`Bins to be tipped`, 1.0)),
                   options = list(scrollX = TRUE),
                   escape = FALSE,
                   rownames = FALSE)
  })
  
  ##############################################################################
  #                            Down load button                                #
  ############################# Closed Batches #################################
  
  closedBatchesTable <- reactive({
    #req(credentials()$user_auth)
    
    GraderBatch |>
      filter(Season == 2025,
             Orchard %in% input$Orchards,
             `Batch closed` == 1) |>
      mutate(Packout = 1-`Reject kgs`/`Input kgs`)
  })
    
    output$closedBatches <- downloadHandler(
      filename = function() {
        paste0("closedBatches-",Sys.Date(),".csv")
      },
      content = function(file) {
        write.csv(closedBatchesTable(), file)
      }
    )
    
 #============================Packout plot======================================
    
   output$packoutPlotTeIpu <- renderPlot({
     #req(credentials()$user_auth)
     
     packOutPlotDataTeIpu <- GraderBatch |>
       filter(Season == 2025,
              `Batch closed` == 1,
              `Packing site` == "Te Ipu Packhouse (RO)") |>
       mutate(`Storage days` = as.numeric(`Pack date`-`Harvest date`),
              Packout = 1-`Reject kgs`/`Input kgs`)
       
      if(input$dateInput == "Storage days") {
        packOutPlotDataTeIpu |>
          filter(!Orchard %in% input$Orchards) |>
          ggplot(aes(x=`Storage days`, y=Packout)) +
          geom_point(colour="#526280", alpha=0.3, size=3) +
          geom_point(data = packOutPlotDataTeIpu |> filter(Orchard %in% input$Orchards),
                      aes(x=`Storage days`, y=Packout), colour="#a9342c", size=4) +
          scale_y_continuous("Packout / %", labels=scales::percent) +
          labs(x = "Storage days") + 
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
        
      } else if(input$dateInput == "Pack date") {
        packOutPlotDataTeIpu |>
          filter(!Orchard %in% input$Orchards) |>
          ggplot(aes(x=`Pack date`, y=Packout)) +
          geom_point(colour="#526280", alpha=0.3, size=3) +
          geom_point(data = packOutPlotDataTeIpu %>% filter(Orchard %in% input$Orchards),
                      aes(x=`Pack date`, y=Packout), colour="#a9342c", size=4) +
          scale_y_continuous("Packout / %", labels=scales::percent) +
          labs(x = "Pack date") + 
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
        
      } else {
        packOutPlotDataTeIpu |>
          filter(!Orchard %in% input$Orchards) |>
          ggplot(aes(x=`Harvest date`, y=Packout)) +
          geom_point(colour="#526280", alpha=0.3, size = 3) +
          geom_point(data = packOutPlotDataTeIpu %>% filter(Orchard %in% input$Orchards),
                      aes(x=`Harvest date`, y=Packout), colour="#a9342c", size=4) +
          scale_y_continuous("Packout / %", labels=scales::percent) +
          labs(x = "Harvest date") + 
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
      }
    })
    
    output$packoutPlotSF <- renderPlot({ 
      #req(credentials()$user_auth)
      
      packOutPlotDataSF <- GraderBatch |>
        filter(Season == 2025,
               `Batch closed` == 1,
               `Packing site` == "Sunfruit Limited") |>
        mutate(`Storage days` = as.numeric(`Pack date`-`Harvest date`),
               Packout = 1-`Reject kgs`/`Input kgs`)
     
      
      packOutPlotDataSF |>
        filter(!Orchard %in% input$Orchards) |> #c("Home Block")) |> #
        ggplot(aes(x=`Pack date`, y=Packout)) +
        geom_point(colour="#526280", alpha=0.3, size=3) +
        geom_point(data = packOutPlotDataSF %>% filter(Orchard %in% input$Orchards), #c("Home Block")),
                   aes(x=`Pack date`, y=Packout), colour="#a9342c", size=4) +
        scale_y_continuous("Packout / %", labels=scales::percent) +
        labs(x = "Pack date") + 
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
    })

#==================================Defect Plot==================================
    
    output$defectPlot <- renderPlot({
      #req(credentials()$user_auth)
      
# Population defect profile      
      
      DA2025pop <- DefectAssessment |>
        filter(Season == 2025) |>
        inner_join(GraderBatch |>
                     filter(Season == 2025,
                            `Batch closed` == 1,
                            `Packing site` == "Te Ipu Packhouse (RO)") |>
                     mutate(Packout = 1-`Reject kgs`/`Input kgs`) |>
                     select(c(GraderBatchID,`Reject kgs`,`Input kgs`)),
                   by = "GraderBatchID") |>
        group_by(Defect) |>
        summarise(DefectQty = sum(DefectQty),
                  SampleQty = sum(SampleQty),
                  `Reject kgs` = sum(`Reject kgs`),
                  `Input kgs` = sum(`Input kgs`)) |>
        mutate(Packout = 1-`Reject kgs`/`Input kgs`,
               defProp = (1-Packout)*DefectQty/SampleQty,
               defPerc = scales::percent(defProp, 0.1),
               Source = "Population") 
      
      Top15 <- DA2025pop |>
        arrange(defProp) |>
        slice_tail(n=15) |>
        pull(Defect)
      
      DA2025RPIN <- DefectAssessment |>
        filter(Season == 2025,
               Orchard %in% input$Orchards) |> #c("Home Block")) |> #
        inner_join(GraderBatch |>
                     filter(Season == 2025,
                            `Batch closed` == 1,
                            `Packing site` == "Te Ipu Packhouse (RO)") |>
                     mutate(Packout = 1-`Reject kgs`/`Input kgs`) |>
                     select(c(GraderBatchID,`Reject kgs`,`Input kgs`)),
                   by = "GraderBatchID") |>
        group_by(Defect) |>
        summarise(DefectQty = sum(DefectQty),
                  SampleQty = sum(SampleQty),
                  `Reject kgs` = sum(`Reject kgs`),
                  `Input kgs` = sum(`Input kgs`)) |>
        mutate(Packout = 1-`Reject kgs`/`Input kgs`,
               defProp = (1-Packout)*DefectQty/SampleQty,
               defPerc = scales::percent(defProp, 0.1),
               Source = "Selected RPIN(s)") 
      
      DA2025pop |>
        bind_rows(DA2025RPIN) |>
        filter(Defect %in% Top15) |>
        mutate(Defect = factor(Defect, levels = Top15)) |>
        ggplot(aes(Defect, defProp, fill=Source)) +
        geom_col(position = position_dodge()) +
        geom_text(aes(label = defPerc, y = defProp), size = 4.0, hjust = -0.2,
                  position = position_dodge(width=0.9), colour = "black") +
        coord_flip() +
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
 
    })
 
    output$defectHeatMap <- renderPlot({
      #req(credentials()$user_auth)
      
      if(nrow(GraderBatch |> 
              filter(Season == 2025,
                     Orchard %in% input$Orchards)) > 0) { 
      
        DA2025byPS <- DefectAssessment |>
          filter(Season == 2025,
                 Orchard %in% input$Orchards) |> #c("Home Block", "Wharerangi Orchard")) |>
          inner_join(GraderBatch |>
                       filter(Season == 2025,
                              `Batch closed` == 1,
                              Orchard %in% input$Orchards) |> #c("Home Block", "Wharerangi Orchard")
                       group_by(GraderBatchID) |>
                       summarise(`Reject kgs` = sum(`Reject kgs`),
                                 `Input kgs` = sum(`Input kgs`)), 
                     by = "GraderBatchID") |>
          group_by(Orchard,`Production site`,Defect) |>
          summarise(DefectQty = sum(DefectQty),
                    SampleQty = sum(SampleQty),
                    `Reject kgs` = sum(`Reject kgs`),
                    `Input kgs` = sum(`Input kgs`),
                    .groups = "drop") |>
          mutate(Packout = 1-`Reject kgs`/`Input kgs`,
                 defProp = (1-Packout)*DefectQty/SampleQty) 
        
        
        top_ten <- DA2025byPS |>
          group_by(Defect) |>
          summarise(DefectQty = sum(DefectQty, na.rm=T),
                    SampleQty = sum(SampleQty, na.rm=T),
                    `Reject kgs` = sum(`Reject kgs`),
                    `Input kgs` = sum(`Input kgs`),
                    .groups = "drop") |>
          mutate(Packout = 1 - `Reject kgs`/`Input kgs`,
                 DefectProp = (1-Packout)*DefectQty/SampleQty) |>
          arrange(desc(DefectProp)) |>
          slice_head(n=10) |>
          pull(Defect)
        
        
        DA2025byPS |>  
          filter(Orchard %in% input$Orchards, #c("Home Block", "Wharerangi Orchard"),
                 Defect %in% top_ten) |>
          mutate(FarmSub = str_c(Orchard," ",`Production site`),
                 Defect = factor(Defect, levels = top_ten)) |>
          ggplot(aes(x=Defect, y=FarmSub)) +
          geom_tile(aes(fill = defProp)) +
          geom_text(aes(label = scales::percent(defProp, accuracy=0.1)), size = 4.0) +
          scale_fill_gradient(low = "white", 
                              high = "#a9342c") +
          labs(x = "Top ten defects", 
               y = "Production site") +
          ggthemes::theme_economist() +
          theme(axis.title.x = element_text(margin = margin(t = 10)),
                axis.title.y = element_text(margin = margin(r = 10)),
                panel.grid.major.x=element_blank(), 
                panel.grid.minor.x=element_blank(), 
                panel.grid.major.y=element_blank(), 
                panel.grid.minor.y=element_blank(),
                axis.text.x = element_text(angle=45, hjust = 1,vjust=1,size = 10),
                axis.text.y = element_text(angle=0, hjust = 1,size = 10),
                plot.title = element_text(size = 14, face = "bold"),
                strip.text = element_text(size = 9),
                plot.background = element_rect(fill = "#F7F1DF", colour = "#F7F1DF"),
                legend.position = "none") 
      } else {
        tibble(x = c(0,1), y = c(0,1)) |>
          ggplot(aes(x = x, y = y)) +
          annotate("text", x=0.5, y=0.5, label = "nothing packed yet", 
                   size = 20, colour = "#526280") +
          theme_void() +
          theme(plot.background = element_rect(fill = "#F7F1DF", colour = "#F7F1DF"))
      }

    })
    
    output$ExcludedMPILots <- DT::renderDataTable({  
      # req(credentials()$user_auth)
      
      PhytoAssSummary <- PhytoAss |>
        filter(Season == 2025) |>
        group_by(GraderBatchMPILotID) |>
        summarise(DefectQty = sum(DefectQty)) 
      
      ExcludedLotsTally <- MPILots |> 
        filter(Season == 2025,
               Orchard %in% input$Orchards) |>
        left_join(PhytoAssSummary, by = "GraderBatchMPILotID") |>
        mutate(DefectQty = replace_na(DefectQty,0),
               ExcludedMPILot = if_else(DefectQty > 0, 1, 0)) |>
        group_by(Orchard, `Production site`) |>
        summarise(`No of MPI lots` = n(),
                  `No of excluded lots` = sum(ExcludedMPILot),
                  .groups = "drop") |>
        mutate(PropExcludedLots = `No of excluded lots`/`No of MPI lots`)
      
      DT:: datatable(ExcludedLotsTally |>
                       mutate(PropExcludedLots = scales::percent(PropExcludedLots, 0.01)), 
                     options = list(scrollX = TRUE),
                     escape = FALSE,
                     rownames = FALSE)
    })
    
    output$PestInterceptions <- renderPlot({
      #req(credentials()$user_auth)
      
      PhytoByPestInterception <- PhytoAss |>
        filter(Season == 2025) |>
        group_by(GraderBatchMPILotID,Defect) |>
        summarise(DefectQty = sum(DefectQty),
                  .groups = "drop") |>
        left_join(MPILots, by = "GraderBatchMPILotID") |>
        filter(Orchard %in% input$Orchards) |>
        group_by(Orchard,`Production site`,Defect) |>
        summarise(DefectQty = sum(DefectQty),
                  .groups = "drop") |>
        mutate(PlotLabel = str_c(Orchard," ",`Production site`))
      
      if(nrow(PhytoByPestInterception) > 0) { 
      
      PhytoByPestInterception |>
        ggplot(aes(x=Defect, y=DefectQty)) +
        geom_col(colour = "#48762e", fill = "#48762e", alpha = 0.5) +
        #coord_flip() +
        facet_wrap(~PlotLabel) +
        scale_fill_manual(values = c("#a9342c","#48762e","#526280","#f6c15f")) +
        scale_colour_manual(values = c("#a9342c","#48762e","#526280","#f6c15f")) +
        labs(y = "No of Interceptions by pest") +
        ggthemes::theme_economist() + 
        theme(legend.position = "top",
              axis.title.x = element_text(margin = margin(t = 10), size = 14),
              axis.title.y = element_text(margin = margin(r = 10), size = 14),
              axis.text.y = element_text(size = 14, hjust=1),
              axis.text.x = element_text(size = 10, angle=45, hjust=1, vjust=1),
              plot.background = element_rect(fill = "#F7F1DF", colour = "#F7F1DF"),
              legend.text = element_text(size = 14),
              legend.title = element_text(size = 14),
              strip.text = element_text(margin = margin(b=10), size = 14))
      
      } else {
        tibble(x = c(0,1), y = c(0,1)) |>
          ggplot(aes(x = x, y = y)) +
          annotate("text", x=0.5, y=0.5, label = "No interceptions yet", 
                   size = 20, colour = "#526280") +
          theme_void() +
          theme(plot.background = element_rect(fill = "#F7F1DF", colour = "#F7F1DF"))
      }
        
        
        })

}
# Run the application 
shinyApp(ui = ui, server = server)
