
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
                      Driver = "SQLServer",#"ODBC Driver 18 for SQL Server", #
                      Server = "abcrepldb.database.windows.net",  
                      Database = "ABCPackerRepl",   
                      UID = "abcadmin",   
                      PWD = "Trauts2018!",
                      Port = 1433
)

BinDelivery <- DBI::dbGetQuery(con, "SELECT * FROM shiny_Bin_DeliveryV")

BinsRemaining <- DBI::dbGetQuery(con, "SELECT * FROM ma_Bins_RemainingV")

GraderBatch <- DBI::dbGetQuery(con, "SELECT * FROM shiny_Grader_BatchV")

PoolDefinition <- DBI::dbGetQuery(con, "SELECT * FROM shiny_Pool_DefintionV")

RTEsFromPTV <- DBI::dbGetQuery(con, "SELECT * FROM fi_Pool_TransactionV")

BinsTipped <- DBI::dbGetQuery(con,
                              "SELECT 
	                                  GraderBatchID
	                                  ,SUM(BinQty) AS BinQty
                              FROM ma_Bin_UsageT
                              WHERE GraderBatchID IS NOT NULL
                              GROUP BY GraderBatchID")

DefectAssessment <- DBI::dbGetQuery(con, "SELECT * FROM shiny_Defect_AssessmentV")

PhytoAss <- DBI::dbGetQuery(con, "SELECT * FROM shiny_Phyto_AssessmentV")

MPILots <- DBI::dbGetQuery(con, "SELECT * FROM shiny_MPI_LotsV")
                           
BinUsage <- DBI::dbGetQuery(con, "SELECT * FROM shiny_Bin_UsageV")
                           
GrowerOrchard <- DBI::dbGetQuery(con,
                                 "SELECT 
                                      FarmName AS Orchard,
                                      CompanyName AS Grower
                                  FROM sw_FarmT AS ft
                                  INNER JOIN
                                      sw_CompanyT AS ct
                                  ON ct.CompanyID = ft.GrowerCompanyID
                                  WHERE ft.ActiveFlag = 1")

BinMovements <- DBI::dbGetQuery(con, "SELECT * FROM ma_Bin_TransfersV")

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

Havelock <- list(Growers = c("ROLP 1", "Rakete","Lawn Road Orchard Limited"),
                 Orchards=c("Stock Roads","Home Block","Manahi","Te Aute Road North","Te Aute Road South","Raukawa","Lobb",
                            "Napier Road North","Napier Road Central","Napier Road South","Haumoana","Lawn Road"))
Maraekakaho <- list(Growers = c("Mana Orchards Limited Partnership","Pioneer Capital Molly Limited","Rockit Orchards Limited",
                                "Heretaunga Orchards Limited Partnership"),
                    Orchards = c("Mana1","Mana2","Pioneer Orchard","Valley Road","Ormond Rd"))
Crownthorpe <- list(Growers = c("Rockit Orchards Limited","Heretaunga Orchards Limited Partnership","Rakete","ROLP 1","ROLP 2"),
                    Orchards = c("Crown","Lowry Heretaunga","Lowry","Rangi2","Sim1","Sim2","Steel","Wharerangi Orchard","Omahu"))
Ceed <- list(Growers = c("ROLP 1","ROLP 2","Rakete","Heretaunga Orchards Limited Partnership","Pioneer Capital Molly Limited",
                         "Rockit Orchards Limited","Mana Orchards Limited Partnership","Lawn Road Orchard Limited"),
            Orchards = c("Home Block","Raukawa","Te Aute Road North","Wharerangi Orchard","Stock Roads","Te Aute Road South",
                         "Napier Road South","Omahu","Haumoana","Napier Road Central","Napier Road North","Rangi2","Lobb","Sim1",
                         "Steel","Manahi","Sim2","Crown","Ormond Rd","Lowry Heretaunga","Pioneer Orchard","Lowry","Valley Road",
                         "Mana1","Mana2","Lawn Road"))
RaketePlus <- list(Growers = c("Rakete","Heretaunga Orchards Limited Partnership","Te Arai Orchard Limited Partnership"),
                   Orchards = c("Sim1","Sim2","Steel","Manahi","Lobb","Ormond Rd","Crown","Lowry Heretaunga","Te Arai"))
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
                             Maraekakaho = Maraekakaho,
                             Crownthorpe = Crownthorpe,
                             Ceed = Ceed,
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
                           tags$p(strong("*Tipped - denotes tipped into Rockit export programme.")),
                           box(title = "Bin summary by poduction site",width = 12, 
                               DT::dataTableOutput("BinAccountingProdSite")),
                           tags$p(strong("*Tipped - denotes tipped into Rockit export programme."))
                         )
                ),
                tabPanel("Bins received",
                         fluidRow(
                           box(title = "Detailed consignment listing",width = 12, 
                               DT::dataTableOutput("ConsignmentDetail")),
                           downloadButton("downloadReceived", "download table", class = "butt1")
                         )
                ),
                tabPanel("Bin storage",
                         tabsetPanel(type = "tabs",
                                     tabPanel("Bin storage by location and type",
                                              fluidRow(
                                                  box(title = "Bin storage by RPIN",width = 12,
                                                      DT::dataTableOutput("StorageByRPIN"))
                                              )
                                     ),
                                     tabPanel("Bins in storage",
                                              fluidRow(
                                                box(title = "Bins currently in storage",width = 12,
                                                    DT::dataTableOutput("BinsRemainingByLoc")),
                                                downloadButton("downloadBIS", "download table", class = "butt1")
                                              )
                                     ),
                                     tabPanel("Bins sent to alternative* channels",
                                              fluidRow(
                                                box(title = "Alternative channel",width = 12,
                                                    DT::dataTableOutput("BinsByAlternativeChannel")),
                                                downloadButton("downloadAC", "download table", class = "butt1")
                                              ),
                                              fluidPage(
                                                tags$p(strong("*Alternative channels are channels for class 1 fruit that do not go into the Rockit export programme")
                                                       )
                                              )
                                     )
                         )
                ),
                tabPanel("Packout",
                         tabsetPanel(type="tabs",
                                     tabPanel("Closed batches",
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
                                                  downloadButton("closedBatches", "download table", class = "butt1")
                                              )
                                     ),
                                     tabPanel("Open batches",
                                              fluidRow(
                                                  box(title = "Open batches by production site",width=12,
                                                  DT::dataTableOutput("OpenBatchesPS"))
                                              )
                                     ),
                                     tabPanel("Packout Summaries",
                                              fluidRow(
                                                tags$h2(strong("\t Packout summaries"),style = "color: #a9342c"),
                                                box(width=4, selectInput("Agglevel", "select the level of aggregation", 
                                                                         c("Grower","Orchard/RPIN","Production site")))
                                              ),
                                              fluidRow(
                                                box(title = "Packout Batch summaries", width=12,
                                                    tags$p(strong("The packouts stated in this table are preliminary and subject to normalisation and potential change"),
                                                           style = "color: #a9342c"),
                                                    DT::dataTableOutput("POSummary")),
                                                downloadButton("PackoutSummary", "download table", class = "butt1")
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
                                     )
                         )
                ),
                tabPanel("RTE Tally",
                         fluidRow(
                           box(width=4, selectInput("RTEAgg", "select the level of aggregation", 
                                                    c("Batch", "Production site", "Orchard/RPIN","Grower")))
                         ),
                         fluidRow(
                           box(title = "RTE Tally",width = 12, 
                               DT::dataTableOutput("RTESummary")),
                           tags$p(strong("*The RTEs stated in the table below are unnormalised and may be adjusted depending on the packing location.
                                         RTEs are only calculated on closed batches.")),
                           downloadButton("RTEDownload", "download table", class = "butt1")
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
                           "DefectDownload", "download table",class = "butt1"
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
  
  BinsAlternate <- c(23,24,25,26,31,32,33)
  BinsDamaged <- c(13:22,27:30,34)
  
  output$BinAccountingRPIN <- DT::renderDataTable({
    #req(credentials()$user_auth)
    
    # Total for the bottom of the table
    
    AlternateChannel <- c("Pickmee Fruit Company Ltd","Fern Ridge","Cedenco")
    
    DamagedBins <- BinsRemaining |> 
      filter(Season == 2025,
             !is.na(BinDamageID),
             BinDamageID %in% BinsDamaged) |>
      group_by(RPIN, Orchard) |>
      summarise(`Damaged bins` = sum(BinQty, na.rm=T),
                .groups = "drop")
    
    BinsRemainingRPIN <- BinsRemaining |> 
      filter(Season == 2025,
             !(BinDamageID %in% BinsDamaged)) |>
      mutate(Channel = if_else(`Current storage site` %in% AlternateChannel,
                               "Alternative",
                               "Storage")) |>
      group_by(RPIN, Orchard, Channel) |>
      summarise(BinQty = sum(BinQty, na.rm=T),
                .groups = "drop") |>
      pivot_wider(id_cols = c(RPIN, Orchard),
                  names_from = c(Channel),
                  values_from = BinQty,
                  values_fill = 0) |>
      rename(`Alternative channel` = Alternative,
             `Currently in storage` = Storage) |>
      relocate(`Currently in storage`, .before = `Alternative channel`) |>
      left_join(DamagedBins, by = c("RPIN","Orchard")) |>
      mutate(`Damaged bins` = replace_na(`Damaged bins`,0))
    
    BRTotal <- BinsRemainingRPIN |>
      filter(Orchard %in% input$Orchards) |> #c("Home Block","Wharerangi Orchard")) |> #
      summarise(`Currently in storage` = sum(`Currently in storage`, na.rm=T),
                `Alternative channel` = sum(`Alternative channel`, na.rm=T),
                `Damaged bins` = sum(`Damaged bins`, na.rm=T))
    
    RPINTotal <- BinDeliveryFull |>
      filter(Orchard %in% input$Orchards, #c("Home Block","Wharerangi Orchard"), #
             Season == 2025) |>
      select(c(RPIN, Orchard, `Bins received`,`In process`,Tipped)) |>
      summarise(`Received` = sum(`Bins received`),
                `In process` = sum(`In process`),
                `Tipped*` = sum(Tipped)) |>
      mutate(RPIN = "Total",
             Orchard = "") |>
      relocate(RPIN, .before = `Received`) |>
      relocate(Orchard, .after = RPIN) |>
      bind_cols(BRTotal)
    
    # Define the table itself
    
    DT::datatable(BinDeliveryFull |>
                    filter(Orchard %in% input$Orchards, #c("Home Block","Wharerangi Orchard"), #
                           Season == 2025) |>
                    select(c(RPIN, Orchard, `Bins received`,`In process`,Tipped)) |>
                    group_by(RPIN, Orchard) |>
                    summarise(`Received` = sum(`Bins received`),
                              `In process` = sum(`In process`),
                              `Tipped*` = sum(Tipped),
                              .groups = "drop") |>
                    ungroup() |>
                    left_join(BinsRemainingRPIN, by =  c("RPIN", "Orchard")) |>
                    bind_rows(RPINTotal) |>
                    mutate(`Currently in storage` = replace_na(`Currently in storage`,0),
                           `Alternative channel` = replace_na(`Alternative channel`,0),
                           `Damaged bins` = replace_na(`Damaged bins`,0)),
                  options = list(scrollX = TRUE),
                  escape = FALSE,
                  rownames = FALSE)
    
  })
  
  # Bin accounting tab....consignment by production site  
  
  output$BinAccountingProdSite <- DT::renderDataTable({  
    #req(credentials()$user_auth)
    
    # Total for the bottom of the table
    
    BinsDamaged <- c(13:22,27:30,34)
    
    DamagedBinsPS <- BinsRemaining |> 
      filter(Season == 2025,
             !is.na(BinDamageID),
             BinDamageID %in% BinsDamaged) |>
      group_by(RPIN, Orchard, `Production site`) |>
      summarise(`Damaged bins` = sum(BinQty, na.rm=T),
                .groups = "drop")
    
    AlternateChannel <- c("Pickmee Fruit Company Ltd","Fern Ridge","Cedenco")
    
    BinsRemainingPS <- BinsRemaining |> 
      filter(Season == 2025,
             !(BinDamageID %in% BinsDamaged)) |>
      mutate(Channel = if_else(`Current storage site` %in% AlternateChannel,
                               "Alternative",
                               "Storage")) |>
      group_by(RPIN, Orchard,`Production site`,Channel) |>
      summarise(BinQty = sum(BinQty, na.rm=T),
                .groups = "drop") |>
      pivot_wider(id_cols = c(RPIN, Orchard, `Production site`),
                  names_from = c(Channel),
                  values_from = BinQty,
                  values_fill = 0) |>
      rename(`Alternative channel` = Alternative,
             `Currently in storage` = Storage) |>
      relocate(`Currently in storage`, .before = `Alternative channel`) |>
      left_join(DamagedBinsPS, by = c("RPIN","Orchard","Production site")) |>
      mutate(`Damaged bins` = replace_na(`Damaged bins`,0))
    
    BRTotalPS <- BinsRemainingPS |>
      filter(Orchard %in% input$Orchards) |> #c("Home Block","Wharerangi Orchard")) |> #
      summarise(`Currently in storage` = sum(`Currently in storage`, na.rm=T),
                `Alternative channel` = sum(`Alternative channel`, na.rm=T),
                `Damaged bins` = sum(`Damaged bins`, na.rm=T))
    
    PSTotal <- BinDeliveryFull |>
      filter(Orchard %in% input$Orchards, #c("Home Block","Wharerangi Orchard"), #
             Season == 2025) |>
      select(c(RPIN, Orchard, `Production site`, `Bins received`,`In process`,Tipped)) |>
      summarise(`Received` = sum(`Bins received`),
                `In process` = sum(`In process`),
                `Tipped*` = sum(Tipped)) |>
      mutate(RPIN = "Total",
             Orchard = "",
             `Production site` = "") |>
      relocate(RPIN, .before = `Received`) |>
      relocate(Orchard, .after = RPIN) |>
      relocate(`Production site`, .after = Orchard) |>
      bind_cols(BRTotalPS)
    
    
    DT::datatable(BinDeliveryFull |>
                    filter(Orchard %in% input$Orchards,#c("Home Block","Wharerangi Orchard"), #
                           Season == 2025) |>
                    select(c(RPIN, Orchard, `Production site`,`Bins received`,`In process`,Tipped)) |>
                    group_by(RPIN,Orchard,`Production site`) |>
                    summarise(`Received` = sum(`Bins received`),
                              `In process` = sum(`In process`),
                              `Tipped*` = sum(Tipped),
                              .groups = "drop") |>
                    left_join(BinsRemainingPS, by =  c("RPIN", "Orchard", "Production site")) |>
                    bind_rows(PSTotal) |>
                    mutate(`Currently in storage` = replace_na(`Currently in storage`,0),
                           `Alternative channel` = replace_na(`Alternative channel`,0),
                           `Damaged bins` = replace_na(`Damaged bins`,0)),
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
                      `Received storage site`,`Storage type`,SmartFreshed)) 
    
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
    
    # Define the table itself
    
    StorageChannel <- c("Te Ipu Packhouse (RO)","Kiwi crunch (FV)","Berl Property Ltd","Sunfruit Limited")
    
    BSByRPIN <- BinsRemaining |>
      filter(Season == 2025,
             `Current storage site` %in% StorageChannel) |>
      select(-`Damage Reason`) |>
      group_by(RPIN, Orchard, `Current storage site`,`Storage type`) |>
      summarise(`Bins Currently in storage` = sum(BinQty, na.rm=T),
                .groups = "drop") |>
      pivot_wider(id_cols = c(RPIN, Orchard, `Current storage site`),
                  names_from = c(`Storage type`),
                  values_from = `Bins Currently in storage`,
                  values_fill = 0) |>
      rename(`Bins currently in storage CA` = CA,
             `Bins currently in storage RA` = RA,
             `Storage site` = `Current storage site`)
    
    FinalTable <- BinDelivery |>
      filter(Orchard %in% input$Orchards, 
             #Orchard %in% c("Sim2"), #
             Season == 2025) |>
      rename(`Storage site` = `Received storage site`) |>
      select(c(RPIN, Orchard, `Storage site`,`Storage type`,`Bins received`)) |>
      group_by(RPIN, Orchard, `Storage site`,`Storage type`) |>
      summarise(`Bins received` = sum(`Bins received`),
                .groups = "drop") |>
      pivot_wider(id_cols = c(RPIN, Orchard, `Storage site`),
                  names_from = `Storage type`,
                  values_from = c(`Bins received`),
                  values_fill = 0) 
    
    if(!("CA" %in% colnames(FinalTable))) {
      FinalTable <- FinalTable |>
        mutate(CA = 0)
    } 
    
    DT::datatable(FinalTable |>
                    rename(`Bins receieved CA` = CA,
                           `Bins receieved RA` = RA) |>
                    left_join(BSByRPIN, by = c("RPIN","Orchard","Storage site")) |>
                    mutate(across(.cols = c(`Bins receieved CA`:`Bins currently in storage RA`), ~replace_na(.x, 0))),
                  options = list(scrollX = TRUE),
                  escape = FALSE,
                  rownames = FALSE)
    
  })
  
  output$BinsRemainingByLoc <- DT::renderDataTable({
    
    StorageChannel <- c("Te Ipu Packhouse (RO)","Kiwi crunch (FV)","Berl Property Ltd","Sunfruit Limited")
    
    BinsRemByLoc <- BinsRemaining |>
      filter(Orchard %in% input$Orchards, 
             `Current storage site` %in% StorageChannel, 
             Season == 2025) |>
      select(-c(BinDeliveryID,`Damage Reason`,BinDamageID)) 
      
    DT::datatable(BinsRemByLoc,
                  options = list(scrollX = TRUE),
                  escape = FALSE,
                  rownames = FALSE)
    
  })
  
  ##################################################################################
  #                            Down load button                                    #
  ############################# Bins received #####################################
  
  BISTable <- reactive({ 
    #req(credentials()$user_auth)
    
    StorageChannel <- c("Te Ipu Packhouse (RO)","Kiwi crunch (FV)","Berl Property Ltd","Sunfruit Limited")
    
    BinsRemByLoc <- BinsRemaining |>
      filter(Orchard %in% input$Orchards, 
             `Current storage site` %in% StorageChannel, 
             Season == 2025) |>
      select(-c(BinDeliveryID,`Damage Reason`))  
    
  })  
  
  
  output$downloadBIS <- downloadHandler(
    filename = function() {
      paste0("BinsInStorage-",Sys.Date(),".csv")
    },
    content = function(file) {
      write.csv(BISTable(), file)
    }
  )
  
  output$BinsByAlternativeChannel <- DT::renderDataTable({
    
    StorageChannel <- c("Te Ipu Packhouse (RO)","Kiwi crunch (FV)","Berl Property Ltd","Sunfruit Limited")
    
    BinsAltChan <- BinsRemaining |>
      filter(Orchard %in% input$Orchards, 
             !(`Current storage site` %in% StorageChannel), 
             Season == 2025) |>
      select(-c(BinDeliveryID, `Storage type`,`Damage Reason`,BinDamageID)) |>
      rename(`Alternative channel` = `Current storage site`)
    
    DT::datatable(BinsAltChan,
                  options = list(scrollX = TRUE),
                  escape = FALSE,
                  rownames = FALSE)
    
  })
  
  ##################################################################################
  #                            Down load button                                    #
  ############################# Bins received #####################################
  
  ACTable <- reactive({ 
    #req(credentials()$user_auth)
    StorageChannel <- c("Te Ipu Packhouse (RO)","Kiwi crunch (FV)","Berl Property Ltd","Sunfruit Limited")
    
    BinsAltChan <- BinsRemaining |>
      filter(Orchard %in% input$Orchards, 
             !(`Current storage site` %in% StorageChannel), 
             Season == 2025) |>
      select(-c(BinDeliveryID, `Storage type`,`Damage Reason`)) |>
      rename(`Alternative channel` = `Current storage site`)
    
  })  
  
  
  output$downloadAC <- downloadHandler(
    filename = function() {
      paste0("AlternativeChannel-",Sys.Date(),".csv")
    },
    content = function(file) {
      write.csv(ACTable(), file)
    }
  )
  
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
             Orchard %in% input$Orchards, 
             #Orchard %in% ("Springhill East"),#
             #`Packing site` == "Te Ipu Packhouse (RO)",
             `Batch closed` == 0) |>
      select(-c(GraderBatchID,Season,Grower,`Batch closed`)) |>
      summarise(`Bins to be tipped` = sum(`Bins tipped`, na.rm=T)) |>
      mutate(`Grader Batch` = "Total",
             RPIN = "",
             Orchard = "",
             `Production site` = "",
             `Packing site` = "") |>
      select(c(`Grader Batch`, RPIN, Orchard, `Production site`,`Packing site`,`Bins to be tipped`))
    
    OpenBatchesFinal <- GraderBatch |>
      filter(Season == 2025,
             Orchard %in% input$Orchards, 
             #Orchard %in% ("Springhill East"),#
             #`Packing site` == "Te Ipu Packhouse (RO)",
             `Batch closed` == 0) |>
      select(-c(GraderBatchID,Grower,`Batch closed`)) |>
      group_by(`Grader Batch`,RPIN,Orchard,`Production site`,`Packing site`) |>
      summarise(`Bins to be tipped` = sum(`Bins tipped`, na.rm=T),
                .groups = "drop") |>
      bind_rows(OpenBatchesPSTotal) |>
      mutate(`Bins to be tipped` = scales::comma(`Bins to be tipped`, 1.0))
    
    DT:: datatable(OpenBatchesFinal,
                   options = list(
                     scrollX = TRUE,
                     columnDefs = list(list(className = 'dt-right', targets = 5))
                     ),
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
  
  #============================Packout Summary Table=============================
  
  output$POSummary <- DT::renderDataTable({  
    #req(credentials()$user_auth)
    
 if(input$Agglevel == "Grower") {   
    
    GrowerList <- GraderBatch |>
      filter(Season == 2025,
             `Batch closed` == 1,
             Orchard  %in% input$Orchards) |>
             #Orchard %in% ("Springhill East")) |>
      distinct(Grower) |>
      pull(Grower)
    
    POSumAgg <- GraderBatch |>
      filter(Season == 2025,
             `Batch closed` == 1,
             Grower  %in% GrowerList) |>
      group_by(Grower) |>
      summarise(`Input kgs` = sum(`Input kgs`, na.rm=T),
                `Reject kgs` = sum(`Reject kgs`, na.rm=T),
                .groups = "drop") |>
      mutate(Packout = 1-`Reject kgs`/`Input kgs`,
             across(.cols = c(`Reject kgs`,`Input kgs`), ~scales::comma(.,1.0)),
             Packout = scales::percent(Packout, 0.01),
             `Packing site` = "Aggregated") |>
      select(c(Grower,`Packing site`,Packout)) 
    
    PackoutSummary <- GraderBatch |>
      filter(Season == 2025,
             `Batch closed` == 1,
             Grower  %in% GrowerList) |>
      group_by(Grower,`Packing site`) |>
      summarise(`Input kgs` = sum(`Input kgs`, na.rm=T),
                `Reject kgs` = sum(`Reject kgs`, na.rm=T),
                .groups = "drop") |>
      mutate(Packout = 1-`Reject kgs`/`Input kgs`,
             across(.cols = c(`Reject kgs`,`Input kgs`), ~scales::comma(.,1.0)),
             Packout = scales::percent(Packout, 0.01)) |>
      select(Grower, `Packing site`, Packout) |>
      bind_rows(POSumAgg) |>
      pivot_wider(id_cols = Grower,
                  names_from = `Packing site`,
                  values_from = Packout,
                  values_fill = NA) |>
      select(-c(Aggregated))
    
    DT:: datatable(PackoutSummary,
                   options = list(scrollX = TRUE),
                   escape = FALSE,
                   rownames = FALSE)
    
 } else if (input$Agglevel == "Orchard/RPIN") { 
   
   POSumAgg <- GraderBatch |>
     filter(Season == 2025,
            `Batch closed` == 1,
            Orchard %in% input$Orchards) |>
            #Orchard %in% ("Springhill East")) |>
     group_by(Grower,RPIN,Orchard) |>
     summarise(`Input kgs` = sum(`Input kgs`, na.rm=T),
               `Reject kgs` = sum(`Reject kgs`, na.rm=T),
               .groups = "drop") |>
     mutate(Packout = 1-`Reject kgs`/`Input kgs`,
            Packout = scales::percent(Packout, 0.01),
            `Packing site` = "Aggregated") |>
     select(c(Grower,RPIN,Orchard,Packout,`Packing site`)) 
   
   PackoutSummary <- GraderBatch |>
     filter(Season == 2025,
            `Batch closed` == 1,
            Orchard %in% input$Orchards) |>
     group_by(Grower, RPIN, Orchard, `Packing site`) |>
     summarise(`Input kgs` = sum(`Input kgs`, na.rm=T),
               `Reject kgs` = sum(`Reject kgs`, na.rm=T),
               .groups = "drop") |>
     mutate(Packout = 1-`Reject kgs`/`Input kgs`,
            Packout = scales::percent(Packout,0.01)) |>
     bind_rows(POSumAgg) |>
     pivot_wider(id_cols = c(Grower,RPIN, Orchard),
                 names_from = c(`Packing site`),
                 values_from = Packout,
                 values_fill = NA) |>
     select(-c(Aggregated))
   
   DT:: datatable(PackoutSummary,
                  options = list(scrollX = TRUE),
                  escape = FALSE,
                  rownames = FALSE)
   
 } else {
   
   POSumAgg <- GraderBatch |>
     filter(Season == 2025,
            `Batch closed` == 1,
            Orchard %in% input$Orchards) |>
     group_by(Grower,RPIN,Orchard,`Production site`) |>
     summarise(`Input kgs` = sum(`Input kgs`, na.rm=T),
               `Reject kgs` = sum(`Reject kgs`, na.rm=T),
               .groups = "drop") |>
     mutate(Packout = 1-`Reject kgs`/`Input kgs`,
            Packout = scales::percent(Packout, 0.01),
            `Packing site` = "Aggregated") |>
     select(c(Grower,RPIN,Orchard,`Production site`,Packout,`Packing site`)) 
   
   PackoutSummary <- GraderBatch |>
     filter(Season == 2025,
            `Batch closed` == 1,
            Orchard %in% input$Orchards) |>
     group_by(Grower, RPIN, Orchard,`Production site`,`Packing site`) |>
     summarise(`Input kgs` = sum(`Input kgs`, na.rm=T),
               `Reject kgs` = sum(`Reject kgs`, na.rm=T),
               .groups = "drop") |>
     mutate(Packout = 1-`Reject kgs`/`Input kgs`,
            Packout = scales::percent(Packout,0.01)) |>
     bind_rows(POSumAgg) |>
     pivot_wider(id_cols = c(Grower,RPIN,Orchard,`Production site`),
                 names_from = c(`Packing site`),
                 values_from = Packout,
                 values_fill = NA) |>
     select(-c(Aggregated))
   
   DT:: datatable(PackoutSummary,
                  options = list(scrollX = TRUE),
                  escape = FALSE,
                  rownames = FALSE)
 }
   
  })
  
  ##################################################################################
  #                            Down load button                                    #
  ############################# Packout Summary#####################################
  
  PackSumTable <- reactive({ 
    #req(credentials()$user_auth)
    
    GraderBatch |>
      filter(Season == 2025,
             `Batch closed` == 1,
             Orchard %in% input$Orchards) |>
      group_by(Grower, RPIN, Orchard,`Production site`,`Packing site`) |>
      summarise(`Input kgs` = sum(`Input kgs`, na.rm=T),
                `Reject kgs` = sum(`Reject kgs`, na.rm=T),
                .groups = "drop") |>
      mutate(Packout = 1-`Reject kgs`/`Input kgs`) 
    
  })  
  
  
  output$PackoutSummary <- downloadHandler(
    filename = function() {
      paste0("PackoutSummary-",Sys.Date(),".csv")
    },
    content = function(file) {
      write.csv(PackSumTable(), file)
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
  
  #===================================RTEs Packed=================================
  
  output$RTESummary <- DT::renderDataTable({  
  
    RTEsByPool <- RTEsFromPTV |>
      filter(Season == 2025) |>
      mutate(`Pool description` = str_sub(`Pool description`,15,-1),
             PoolDesc = case_when(`Pool description` %in% c("0","140","145","160","165") ~ "Mixed",
                                  TRUE ~ `Pool description`),
             GraderBatchID = coalesce(GraderBatchID, PresizeGraderBatchID)) |>
      group_by(GraderBatchID, PoolDesc) |>
      summarise(RTEs = sum(TransRTE, na.rm=T),
                .groups = "drop") 
      
  
    RTEByGB <- GraderBatch |>
      # Filtering for closed batches only
      filter(`Batch closed` == 1,
             Season == 2025) |>
      left_join(RTEsByPool, by = "GraderBatchID") |>
      filter(!is.na(PoolDesc)) |>
      group_by(GraderBatchID, `Grader Batch`,Grower,RPIN,Orchard,`Production site`,PoolDesc) |>
      summarise(RTEs = sum(RTEs, na.rm=T),
                .groups = "drop") |>
      pivot_wider(id_cols = c(GraderBatchID, `Grader Batch`,Grower,RPIN,Orchard,`Production site`),
                  names_from = PoolDesc,
                  values_from = RTEs,
                  values_fill = 0) |>
      rowwise() |>
      mutate(`53` = 0,
             TotalRTEs = sum(c_across(`58`:Mixed))) |>
      relocate(`53`, .before = `58`) |>
      left_join(BinsTipped, by = "GraderBatchID") |>
      mutate(RTEsPerBin = TotalRTEs/BinQty) |>
      filter(!is.na(BinQty)) |>
      arrange(`Grader Batch`)
    
    if (input$RTEAgg == "Batch") {
      
      RTEsByGBParsedTotal <- RTEByGB |>
        filter(Orchard %in% input$Orchards) |>
        #filter(Orchard %in% c("Springhill East")) |>
        ungroup() |>
        summarise(across(.cols = c(`53`:BinQty), ~sum(., na.rm=T))) |>
        mutate(RTEsPerBin = TotalRTEs/BinQty,
               `Grader Batch` = "Total",
               Grower = "",
               RPIN = "",
               Orchard = "",
               `Production site` = "") |>
        relocate(`Grader Batch`, .before = `58`) |>
        relocate(Grower, .after = `Grader Batch`) |>
        relocate(RPIN, .after = Grower) |>
        relocate(Orchard, .after = RPIN) |> 
        relocate(`53`, .before = `58`) |>
        relocate(`Production site`, .after = Orchard)
      
      RTEsByGBParsed <- RTEByGB |>
        filter(Orchard %in% input$Orchards) |>
        #filter(Orchard %in% c("Springhill East")) |>
        select(-c(GraderBatchID)) |>
        arrange(`Grader Batch`) |>
        bind_rows(RTEsByGBParsedTotal) |>
        rename(`Mixed*`= Mixed,
               `Total RTEs*` = TotalRTEs) |>
        mutate(across(.cols = c(`58`:RTEsPerBin), ~scales::comma(.,1.0))) |>
        select(-c(`53`:`Mixed*`))
        
        
      
      DT::datatable(RTEsByGBParsed,
                     options = list(
                       columnDefs = list(list(className = 'dt-right', targets = 5:7)),
                       scrollX = TRUE
                     ),
                     escape = FALSE,
                     rownames = FALSE) |>
        DT::formatStyle(
          0,
          target="row",
          fontWeight = DT::styleRow(c(nrow(RTEsByGBParsed)),"bold")
        )
      
    } else if (input$RTEAgg == "Production site") {

      RTEsByPSParsedTotal <- RTEByGB |>
        filter(Orchard %in% input$Orchards) |>
        #filter(Orchard %in% c("Springhill East")) |>
        ungroup() |>
        summarise(across(.cols = c(`53`:BinQty), ~sum(., na.rm=T))) |>
        mutate(RTEsPerBin = TotalRTEs/BinQty,
               Grower = "Total",
               RPIN = "",
               Orchard = "",
               `Production site` = "") |>
        relocate(Grower, .before = `58`) |>
        relocate(RPIN, .after = Grower) |>
        relocate(Orchard, .after = RPIN) |>
        relocate(`53`, .before = `58`) |>
        relocate(`Production site`, .after = Orchard)
            
      RTEsByPSParsed <- RTEByGB |>
        filter(Orchard %in% input$Orchards) |>
        #filter(Orchard %in% c("Springhill East")) |>
        group_by(Grower,RPIN,Orchard,`Production site`) |>
        summarise(across(.cols = c(`53`:BinQty), ~sum(.,na.rm=T)),
                  .groups = "drop") |>
        mutate(RTEsPerBin = TotalRTEs/BinQty) |>
        bind_rows(RTEsByPSParsedTotal) |>
        rename(`Mixed*`= Mixed,
               `Total RTEs*` = TotalRTEs) |>
        mutate(across(.cols = c(`58`:RTEsPerBin), ~scales::comma(.,1.0))) |>
        select(-c(`53`:`Mixed*`))
      
      DT:: datatable(RTEsByPSParsed,
                     options = list(
                       columnDefs = list(list(className = 'dt-right', targets = 4:6)),
                       scrollX = TRUE
                     ),
                     escape = FALSE,
                     rownames = FALSE) |>
        DT::formatStyle(
          0,
          target="row",
          fontWeight = DT::styleRow(c(nrow(RTEsByPSParsed)),"bold")
        )
      
    } else if (input$RTEAgg == "Orchard/RPIN") {

      RTEsByRPINParsedTotal <- RTEByGB |>
        filter(Orchard %in% input$Orchards) |>
        #filter(Orchard %in% c("Springhill East")) |>
        ungroup() |>
        summarise(across(.cols = c(`53`:BinQty), ~sum(., na.rm=T))) |>
        mutate(RTEsPerBin = TotalRTEs/BinQty,
               Grower = "Total",
               RPIN = "",
               Orchard = "") |>
        relocate(Grower, .before = `58`) |>
        relocate(RPIN, .after = Grower) |>
        relocate(Orchard, .after = RPIN) |>
        relocate(`53`, .before = `58`) 
            
      RTEsByRPINParsed <- RTEByGB |>
        filter(Orchard %in% input$Orchards) |>
        #filter(Orchard %in% c("Springhill East")) |>
        group_by(Grower,RPIN,Orchard) |>
        summarise(across(.cols = c(`53`:BinQty), ~sum(.,na.rm=T)),
                  .groups = "drop") |>
        mutate(RTEsPerBin = TotalRTEs/BinQty) |>
        bind_rows(RTEsByRPINParsedTotal) |>
        rename(`Mixed*`= Mixed,
               `Total RTEs*` = TotalRTEs) |>
        mutate(across(.cols = c(`58`:RTEsPerBin), ~scales::comma(.,1.0))) |>
        select(-c(`53`:`Mixed*`))
      
      DT:: datatable(RTEsByRPINParsed,
                     options = list(
                       columnDefs = list(list(className = 'dt-right', targets = 3:5)),
                       scrollX = TRUE
                     ),
                     escape = FALSE,
                     rownames = FALSE) |>
        DT::formatStyle(
          0,
          target="row",
          fontWeight = DT::styleRow(c(nrow(RTEsByRPINParsed)),"bold")
        )
      
    } else {
      
      GrowerList <- RTEByGB |>
        filter(Orchard %in% input$Orchards) |>
        #filter(Orchard %in% c("Springhill East")) |>
        distinct(Grower) |>
        pull(Grower)
      
      RTEsByGrowerParsedTotal <- RTEByGB |>
        filter(Grower %in% GrowerList) |>
        ungroup() |>
        summarise(across(.cols = c(`53`:BinQty), ~sum(., na.rm=T))) |>
        mutate(RTEsPerBin = TotalRTEs/BinQty,
               Grower = "Total") |>
        relocate(Grower, .before = `58`) |>
        relocate(`53`, .before = `58`) 
      
      RTEsByGrowerParsed <- RTEByGB |>
        filter(Grower %in% GrowerList) |>
        group_by(Grower) |>
        summarise(across(.cols = c(`53`:BinQty), ~sum(.,na.rm=T)),
                  .groups = "drop") |>
        mutate(RTEsPerBin = TotalRTEs/BinQty) |>
        bind_rows(RTEsByGrowerParsedTotal) |>  
        rename(`Mixed*`= Mixed,
               `Total RTEs*` = TotalRTEs) |>
        mutate(across(.cols = c(`58`:RTEsPerBin), ~scales::comma(.,1.0))) |>
        select(-c(`53`:`Mixed*`))
      
      DT::datatable(RTEsByGrowerParsed,
                     options = list(
                       columnDefs = list(list(className = 'dt-right', targets = 1:3)),
                       scrollX = TRUE
                       ),
                     escape = FALSE,
                     rownames = FALSE) |>
        DT::formatStyle(
          0,
          target="row",
          fontWeight = DT::styleRow(c(nrow(RTEsByGrowerParsed)),"bold")
        )
      
      
    }
      
    
    
  })
  
  ##################################################################################
  #                            Down load button                                    #
  ############################# RTE  Summary   #####################################
  
  RTESumTable <- reactive({ 
    #req(credentials()$user_auth)
    
    RTEsByPool <- RTEsFromPTV |>
      filter(Season == 2025) |>
      mutate(`Pool description` = str_sub(`Pool description`,15,-1),
             PoolDesc = case_when(`Pool description` %in% c("0","140","145","160","165") ~ "Mixed",
                                  TRUE ~ `Pool description`),
             GraderBatchID = coalesce(GraderBatchID, PresizeGraderBatchID)) |>
      group_by(GraderBatchID, PoolDesc) |>
      summarise(RTEs = sum(TransRTE, na.rm=T),
                .groups = "drop") 
   
    RTEByGB <- GraderBatch |>
      # Filtering for closed batches only
      filter(`Batch closed` == 1,
             Season == 2025) |>
      left_join(RTEsByPool, by = "GraderBatchID") |>
      filter(!is.na(PoolDesc)) |>
      group_by(GraderBatchID, `Grader Batch`,Grower,RPIN,Orchard,`Production site`,PoolDesc) |>
      summarise(RTEs = sum(RTEs, na.rm=T),
                .groups = "drop") |>
      pivot_wider(id_cols = c(GraderBatchID, `Grader Batch`,Grower,RPIN,Orchard,`Production site`),
                  names_from = PoolDesc,
                  values_from = RTEs,
                  values_fill = 0) |>
      rowwise() |>
      mutate(`53` = 0,
             TotalRTEs = sum(c_across(`58`:Mixed))) |>
      relocate(`53`, .before = `58`) |>
      left_join(BinsTipped, by = "GraderBatchID") |>
      mutate(RTEsPerBin = TotalRTEs/BinQty) |>
      filter(!is.na(BinQty)) |>
      arrange(`Grader Batch`)
    
    RTEByGB |>
      filter(Orchard %in% c("Springhill East")) |>
      #filter(Orchard %in% input$Orchards) |>
      rowwise() |>
      filter(!is.na(BinQty)) |>
      select(-c(GraderBatchID)) |>
      arrange(`Grader Batch`)
    
  })  
  
  
  output$RTEDownload <- downloadHandler(
    filename = function() {
      paste0("RTESummary-",Sys.Date(),".csv")
    },
    content = function(file) {
      write.csv(RTESumTable(), file)
    }
  )
  
  
  #==================================Defect Plot==================================
  
  output$defectPlot <- renderPlot({
    #req(credentials()$user_auth)
    
    # Population defect profile 
    
    SampQtyPop <- DefectAssessment |>
      filter(Season == 2025) |>
      inner_join(GraderBatch |>
                   filter(Season == 2025,
                          `Batch closed` == 1,
                          `Packing site` == "Te Ipu Packhouse (RO)") |>
                   select(c(GraderBatchID)),
                 by = "GraderBatchID") |>
      group_by(GraderBatchID) |>
      summarise(SampleQty = max(SampleQty, na.rm=T),
                .groups = "drop") |>
      summarise(SampleQty = sum(SampleQty, na.rm=T))
    
    POPop <- GraderBatch |>
      filter(Season == 2025,
             `Batch closed` == 1,
             `Packing site` == "Te Ipu Packhouse (RO)") |>
      summarise(`Input kgs` = sum(`Input kgs`, na.rm=T),
                `Reject kgs` = sum(`Reject kgs`, na.rm=T)) |>
      mutate(Packout = 1-`Reject kgs`/`Input kgs`) |>
      select(-c(`Reject kgs`,`Input kgs`))
    
    
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
      summarise(DefectQty = sum(DefectQty)) |>
      mutate(SampleQty = SampQtyPop[[1]],
             Packout = POPop[[1]],
             defProp = (1-Packout)*DefectQty/SampleQty,
             defPerc = scales::percent(defProp, 0.1),
             Source = "Population") 
    
    Top15 <- DA2025pop |>
      arrange(defProp) |>
      slice_tail(n=15) |>
      pull(Defect)
    
    SampQtyRPIN <- DefectAssessment |>
      filter(Season == 2025,
             #Orchard %in% c("Home Block", "Stock Roads")) |> 
             Orchard %in% input$Orchards) |>
      inner_join(GraderBatch |>
                   filter(Season == 2025,
                          `Batch closed` == 1,
                          `Packing site` == "Te Ipu Packhouse (RO)") |>
                   mutate(Packout = 1-`Reject kgs`/`Input kgs`) |>
                   select(c(GraderBatchID,`Reject kgs`,`Input kgs`)),
                 by = "GraderBatchID") |>
      group_by(Orchard, GraderBatchID) |>
      summarise(SampleQty = max(SampleQty),
                .groups = "drop") |>
      summarise(SampleQty = sum(SampleQty))
    
    PORPIN <- GraderBatch |>
      filter(Season == 2025,
             `Batch closed` == 1,
             `Packing site` == "Te Ipu Packhouse (RO)",
             #Orchard %in% c("Home Block", "Stock Roads")) |> 
             Orchard %in% input$Orchards) |>
      summarise(`Input kgs` = sum(`Input kgs`, na.rm=T),
                `Reject kgs` = sum(`Reject kgs`, na.rm=T)) |>
      mutate(Packout = 1-`Reject kgs`/`Input kgs`) |>
      select(-c(`Reject kgs`,`Input kgs`))
    
    DA2025RPIN <- DefectAssessment |>
      filter(Season == 2025,
             #Orchard %in% c("Home Block", "Stock Roads")) |> 
             Orchard %in% input$Orchards) |>
      inner_join(GraderBatch |>
                   filter(Season == 2025,
                          `Batch closed` == 1,
                          `Packing site` == "Te Ipu Packhouse (RO)") |>
                   mutate(Packout = 1-`Reject kgs`/`Input kgs`) |>
                   select(c(GraderBatchID,`Reject kgs`,`Input kgs`)),
                 by = "GraderBatchID") |>
      group_by(Defect) |>
      summarise(DefectQty = sum(DefectQty)) |>
      mutate(SampleQty = SampQtyRPIN[[1]],
             Packout = PORPIN[[1]],
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
      
      SampQtyByPS <- DefectAssessment |>
        filter(Season == 2025,
               #Orchard %in% c("Home Block", "Stock Roads")) |> 
               Orchard %in% input$Orchards) |>
        group_by(Orchard, `Production site`, GraderBatchID) |>
        summarise(SampleQty = max(SampleQty, na.rm=T),
                  .groups = "drop") |>
        group_by(Orchard, `Production site`) |>
        summarise(SampleQty = sum(SampleQty),
                  .groups = "drop")
      
      POByPS <- GraderBatch |>
        filter(Season == 2025,
               `Batch closed` == 1,
               #Orchard %in% c("Home Block", "Stock Roads"),
               Orchard %in% input$Orchards,
               `Packing site` == "Te Ipu Packhouse (RO)") |>
        group_by(Orchard, `Production site`) |>
        summarise(`Reject kgs` = sum(`Reject kgs`),
                  `Input kgs` = sum(`Input kgs`),
                  .groups = "drop") |>
        mutate(Packout = 1-`Reject kgs`/`Input kgs`) |>
        select(-c(`Reject kgs`,`Input kgs`))
      
      
      DA2025byPS <- DefectAssessment |>
        filter(Season == 2025,
               #Orchard %in% c("Home Block", "Stock Roads")) |> 
               Orchard %in% input$Orchards) |>
        inner_join(GraderBatch |>
                     filter(Season == 2025,
                            `Batch closed` == 1,
                            #Orchard %in% c("Home Block", "Stock Roads"),
                            Orchard %in% input$Orchards,
                            `Packing site` == "Te Ipu Packhouse (RO)") |>
                     select(c(GraderBatchID)), 
                   by = "GraderBatchID") |>
        select(-SampleQty) |>
        group_by(Orchard,`Production site`,Defect) |>
        summarise(DefectQty = sum(DefectQty),
                  .groups = "drop") |>
        left_join(SampQtyByPS, by = c("Orchard","Production site")) |>
        left_join(POByPS, by = c("Orchard","Production site")) |>
        mutate(defProp = (1-Packout)*DefectQty/SampleQty) 
      
      SampQtyPop <- DefectAssessment |>
        filter(Season == 2025) |>
        inner_join(GraderBatch |>
                     filter(Season == 2025,
                            `Batch closed` == 1,
                            `Packing site` == "Te Ipu Packhouse (RO)") |>
                     select(c(GraderBatchID)),
                   by = "GraderBatchID") |>
        group_by(GraderBatchID) |>
        summarise(SampleQty = max(SampleQty, na.rm=T),
                  .groups = "drop") |>
        summarise(SampleQty = sum(SampleQty, na.rm=T))
      
      POPop <- GraderBatch |>
        filter(Season == 2025,
               `Batch closed` == 1,
               `Packing site` == "Te Ipu Packhouse (RO)") |>
        summarise(`Input kgs` = sum(`Input kgs`, na.rm=T),
                  `Reject kgs` = sum(`Reject kgs`, na.rm=T)) |>
        mutate(Packout = 1-`Reject kgs`/`Input kgs`) |>
        select(-c(`Reject kgs`,`Input kgs`))
      
      
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
        summarise(DefectQty = sum(DefectQty)) |>
        mutate(SampleQty = SampQtyPop[[1]],
               Packout = POPop[[1]],
               defProp = (1-Packout)*DefectQty/SampleQty,
               defPerc = scales::percent(defProp, 0.1),
               Source = "Population") 
      
      top_ten <- DA2025pop |>
        arrange(desc(defProp)) |>
        slice_head(n=10) |>
        pull(Defect)
      
      DA2025byPS |>  
        filter(Defect %in% top_ten) |>
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
  
  ##################################################################################
  #                            Down load button                                    #
  ############################# Defect  Summary ####################################
  
  DefectSumTable <- reactive({ 
    #req(credentials()$user_auth)
    
    SampQtyPop <- DefectAssessment |>
      filter(Season == 2025) |>
      inner_join(GraderBatch |>
                   filter(Season == 2025,
                          `Batch closed` == 1,
                          `Packing site` == "Te Ipu Packhouse (RO)") |>
                   select(c(GraderBatchID)),
                 by = "GraderBatchID") |>
      group_by(GraderBatchID) |>
      summarise(SampleQty = max(SampleQty, na.rm=T),
                .groups = "drop") |>
      summarise(SampleQty = sum(SampleQty, na.rm=T))
    
    POPop <- GraderBatch |>
      filter(Season == 2025,
             `Batch closed` == 1,
             `Packing site` == "Te Ipu Packhouse (RO)") |>
      summarise(`Input kgs` = sum(`Input kgs`, na.rm=T),
                `Reject kgs` = sum(`Reject kgs`, na.rm=T)) |>
      mutate(Packout = 1-`Reject kgs`/`Input kgs`) |>
      select(-c(`Reject kgs`,`Input kgs`))
    
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
      summarise(DefectQty = sum(DefectQty)) |>
      mutate(SampleQty = SampQtyPop[[1]],
             Packout = POPop[[1]],
             defProp = (1-Packout)*DefectQty/SampleQty,
             defPerc = scales::percent(defProp, 0.1),
             Orchard = "Population") |>
      relocate(Orchard, .before = Defect)
    
    SampQtyRPIN <- DefectAssessment |>
      filter(Season == 2025,
             #Orchard %in% c("Home Block", "Stock Roads")) |> 
             Orchard %in% input$Orchards) |>
      inner_join(GraderBatch |>
                   filter(Season == 2025,
                          `Batch closed` == 1,
                          `Packing site` == "Te Ipu Packhouse (RO)") |>
                   mutate(Packout = 1-`Reject kgs`/`Input kgs`) |>
                   select(c(GraderBatchID,`Reject kgs`,`Input kgs`)),
                 by = "GraderBatchID") |>
      group_by(Orchard, GraderBatchID) |>
      summarise(SampleQty = max(SampleQty),
                .groups = "drop") |>
      summarise(SampleQty = sum(SampleQty))
    
    PORPIN <- GraderBatch |>
      filter(Season == 2025,
             `Batch closed` == 1,
             `Packing site` == "Te Ipu Packhouse (RO)",
             #Orchard %in% c("Home Block", "Stock Roads")) |> 
             Orchard %in% input$Orchards) |>
      summarise(`Input kgs` = sum(`Input kgs`, na.rm=T),
                `Reject kgs` = sum(`Reject kgs`, na.rm=T)) |>
      mutate(Packout = 1-`Reject kgs`/`Input kgs`) |>
      select(-c(`Reject kgs`,`Input kgs`))
    
    DefectAssessment |>
      filter(Season == 2025,
             #Orchard %in% c("Home Block", "Stock Roads")) |> 
             Orchard %in% input$Orchards) |>
      inner_join(GraderBatch |>
                   filter(Season == 2025,
                          `Batch closed` == 1,
                          `Packing site` == "Te Ipu Packhouse (RO)") |>
                   mutate(Packout = 1-`Reject kgs`/`Input kgs`) |>
                   select(c(GraderBatchID,`Reject kgs`,`Input kgs`)),
                 by = "GraderBatchID") |>
      group_by(Orchard,Defect) |>
      summarise(DefectQty = sum(DefectQty)) |>
      mutate(SampleQty = SampQtyRPIN[[1]],
             Packout = PORPIN[[1]],
             defProp = (1-Packout)*DefectQty/SampleQty,
             defPerc = scales::percent(defProp, 0.1)) |>
      bind_rows(DA2025pop)
    
  })  
  
  
  output$DefectDownload <- downloadHandler(
    filename = function() {
      paste0("DefectSummary-",Sys.Date(),".csv")
    },
    content = function(file) {
      write.csv(DefectSumTable(), file)
    }
  )
  
  
}
# Run the application 
shinyApp(ui = ui, server = server)
