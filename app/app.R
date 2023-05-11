# load libraries
library(shiny)
library(shinyjs)
library(leaflet)
library(jsonlite)
library(redux)

Sys.setenv(TZ='UTC')

# timw window used for plotting
time_window <- 24*3600

log <- function(message){
    if( is.list(message) ){
        stop( message )
    }else{
        cat( file=stderr(), message )
    }
}


# Define UI
ui <- bootstrapPage(
  tags$head(
    tags$style(type = "text/css", "#map {height: calc(100vh - 0px) !important;}"),
    tags$style(HTML(".leaflet-container { background: #e6f2ff; }")),
    tags$style(type="text/css", "#map.recalculating { opacity: 1.0 !important; }"),
    tags$style(type="text/css", "#connection_status.recalculating { opacity: 1.0!important; }")),
  leafletOutput("map"),
  absolutePanel(
    style = "background-color: white; opacity: 0.8",
    top = "2%", left = "2%",
    htmlOutput("connection_status")
  )
)

# Define server logic required to draw a histogram
server <- function(input, output, session) {
  log("Connecting to redis\n")
  connection <- redux::hiredis(host="redis",port=6379)
  log("Setting up map\n")
  # setup map
  map_status <- reactiveVal(FALSE)
  output$map <- renderLeaflet({
    m <- leaflet(options = leafletOptions(zoomControl = FALSE)) %>% addTiles()
    m <- m %>% setView(lat = 0, lng = 0, zoom = 2) %>%
      setMaxBounds( lng1 = -180
                , lat1 = -90
                , lng2 = 180
                , lat2 = 90 )
    log("map set up\n")
    m
  })

  # reactive to hold cache of messages
  values <- reactiveValues(obs = data.frame() )
  num_messages <- reactiveVal(value=0)
  log("Setting up observer\n")
  observe({
    invalidateLater(1000*30, session) # update every 30 seconds
      if( map_status() ){
      isolate({
        log(paste0(Sys.time(), ": Fetching data ...\n"))
        min_time <- Sys.time() - time_window
        min_score <- as.numeric(format(min_time, "%s"))
        ids <- connection$ZRANGEBYSCORE("default",min_score,"+inf")
        values$obs <- do.call('rbind',lapply( ids, FUN = function(X) { as.data.frame(fromJSON(connection$GET(X), simplifyVector=FALSE))}))
        if( ! is.null(values$obs) ){
          if( nrow(values$obs) > 0 ){
            values$obs <- subset(values$obs, abs(longitude) < 180 & abs(latitude) < 90)
            num_messages(nrow(values$obs))
          }
        }
      })
    }
  })

  # observer to detect when map ready
  observeEvent(input$map_zoom, {
    map_status(TRUE)
  })

  # update clock
  observe({
    invalidateLater(1000,session) # update every second
    isolate({
      time_now <- format.POSIXct(Sys.time()-time_window,"%Y-%m-%d %H:%M:%S UTC")
      msg <- "<h4>WIS2 pilot<br/>Surface stations reporting past 24 hour</h4>"
      output$connection_status <- renderText(msg)
    })
  })

  # update map on data changes
  observe({
    obs <- values$obs
    if( !is.null(obs)){
      if( nrow(obs) > 0){
        m <- leafletProxy("map") %>% clearGroup("obs") %>%
                  addCircleMarkers(lat = obs$latitude, lng = obs$longitude, radius = 5, stroke=TRUE,
                                   weight=1, color="black", fillColor = "blue", fillOpacity = 0.5, group="obs")
      }
    }
  })
}
# Run the application
shinyApp(ui, server)
