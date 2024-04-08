# load libraries
library(shiny)
library(shinyjs)
library(leaflet)
library(leaflet.extras)
library(jsonlite)
library(redux)
library(shinydashboard)

Sys.setenv(TZ='UTC')

# timw window used for plotting
time_window <- 24*3600

logmsg <- function(message){
    if( is.list(message) ){
        stop( message )
    }else{
        cat( file=stderr(), message )
    }
}


# Define UI
ui <- dashboardPage(
  dashboardHeader(disable = FALSE, title = "WIS2.0 Pilot (Surface stations reporting past 24 hours)", titleWidth=600),
  dashboardSidebar(disable = TRUE),
  dashboardBody(
    tags$head(
      tags$style(type = "text/css", "#map {height: calc(100vh - 80px) !important;}"),
      tags$style(HTML(".leaflet-container { background: #e6f2ff; }")),
      tags$style(type="text/css", "#map.recalculating { opacity: 1.0 !important; }"),
      tags$style(type="text/css", ".shiny-notification {position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%); z-index: 9999; }")
    ),
    leafletOutput("map")
  )
)


# Define server logmsgic required to draw a histogram
server <- function(input, output, session) {
  logmsg("Connecting to redis\n")
  connection <- redux::hiredis(host="redis",port=6379)
  logmsg("Setting up map\n")
  # setup map
  map_status <- reactiveVal(FALSE)
  output$map <- renderLeaflet({
    m <- leaflet(options = leafletOptions(zoomControl = FALSE)) %>%
             addProviderTiles("Esri.WorldShadedRelief")
    m <- m %>% setView(lat = 0, lng = 0, zoom = 2) %>%
      setMaxBounds( lng1 = -270
                , lat1 = -90
                , lng2 = 270
                , lat2 = 90 )
    logmsg("map set up\n")
    m
  })

  # reactive to hold cache of messages
  values <- reactiveValues(obs = data.frame() )
  num_messages <- reactiveVal(value=0)
  logmsg("Setting up observer\n")
  observe({
    invalidateLater(1000*300, session) # update every 30 seconds
      if( map_status() ){
      isolate({
        logmsg(paste0(Sys.time(), ": Fetching data ...\n"))
        min_time <- Sys.time() - time_window
        min_score <- as.numeric(format(min_time, "%s"))
        ids <- connection$ZRANGEBYSCORE("default",min_score,"+inf")
        values$obs <- do.call('rbind',lapply( ids, FUN = function(X) {
          obs <- fromJSON(connection$GET(X), simplifyVector=FALSE)
          obs <- lapply(obs, FUN = function(X){
            rval <- ifelse(is.null(X), NA, X)
          })
          as.data.frame(obs)
        }))
        if( ! is.null(values$obs) ){
          if( nrow(values$obs) > 0 ){
            # wrap
            east <- values$obs[which( values$obs$longitude > 0),]
            east$longitude <- east$longitude - 360
            west <- values$obs[which( values$obs$longitude < 0),]
            west$longitude <- west$longitude + 360
            values$obs <- rbind(west, values$obs)
            values$obs <- rbind(values$obs, east)
            trimmed <- nrow(values$obs)
            values$obs <- subset(values$obs, abs(longitude) < 360 & abs(latitude) < 90)
            trimmed <- trimmed - nrow(values$obs)
            logmsg(paste0("Observations trimmed due to missing location: ", trimmed))
            num_messages(nrow(values$obs))
          }
        }
      })
    }
  })


  notification_shown <- reactiveVal(FALSE)
  # observer to detect when map ready
  observeEvent(input$map_zoom, {
    if( ! notification_shown() ){
      showNotification(HTML("<body><h3>Maps Disclaimer</h3><p>The designations employed in this website are in conformity with United Nations practice.
      The presentation of material therein does not imply the expression of any opinion whatsoever on the part of WMO concerning the legal status of any
      country, area or territory or of its authorities, or concerning the delimitation of its borders. The depiction and use of boundaries, geographic
      names and related data shown on maps and included in lists, tables, documents, and databases on this website are not warranted to be error free
      nor do they necessarily imply official endorsement or acceptance by the WMO.</p>
      <h3>About</h3>
      <p>Every point shown on the map indicates the location of a station where an observation has been received in the last 24 hours via the WIS2.0 pilot phase,
      with locations taken from the decoded BUFR messages. Please note, due to the large number of stations there may be a short delay before the first data
      appear. The map auto refreshes every 5 minutes.</p>
      <p>Click on the x to close this window.</p>
      </body>"), duration=30, type = "message")
      notification_shown(TRUE)
    }
    map_status(TRUE)
  })

  # update map on data changes
  observe({
    obs <- values$obs
    if( !is.null(obs)){
      if( nrow(obs) > 0){
        obstime <- sprintf("%04d-%02d-%02d %02d:%02dZ", obs$year, obs$month, obs$day, obs$hour, obs$minute)
        obsid <- paste(obs$wsi_series, obs$wsi_issuer, obs$wsi_issue_number, obs$wsi_local_identifier, sep="-")
        label <- paste0("(",obstime,") ", obsid)
        m <- leafletProxy("map") %>% clearGroup("obs") %>%
                  addCircleMarkers(lat = obs$latitude, lng = obs$longitude, radius = 5*obs$plot_size, stroke=TRUE, label = label,
                                   weight=1, color=obs$plot_colour, fillColor = obs$plot_colour, fillOpacity = 0.5, group="obs")
      }
    }
  })
}
# Run the application
shinyApp(ui, server)
