# load libraries
library(shiny)
library(shinyjs)
library(leaflet)
library(jsonlite)
library(redux)

Sys.setenv(TZ='UTC')

id <- "7d7d559f-667d-4cf5-9319-bbb3366f4891"

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
  output$map <- renderLeaflet({
    m <- leaflet(options = leafletOptions(zoomControl = FALSE)) %>% addTiles()
    m <- m %>% setView(lat = 0, lng = 0, zoom = 2)
    log("map set up\n")
    m
  })
  log("Setting up reactive values\n")
  # reactive to hold cache of messages
  values <- reactiveValues(obs = data.frame() )
  num_messages <- reactiveVal(value=0)
  log("Setting up observer\n")
  observe({
    invalidateLater(1000*30, session) # update every 30 seconds
      isolate({
        log(paste0(Sys.time(), ": Fetching data ...\n"))
        min_time <- Sys.time() - 60*60
        min_score <- as.numeric(format(min_time, "%s"))
        ids <- connection$ZRANGEBYSCORE("default",min_score,"+inf")
        log(paste0(ids,"\n"))
        values$obs <- do.call('rbind',lapply( ids, FUN = function(X) { as.data.frame(fromJSON(connection$GET(X), simplifyVector=FALSE))}))
        if( ! is.null(values$obs) ){
          if( nrow(values$obs) > 0 ){
            values$obs <- subset(values$obs, abs(longitude) < 180 & abs(latitude) < 90)
            num_messages(nrow(values$obs))
          }
        }
      })
  })

  # update clock
  observe({
    invalidateLater(1000,session)
    isolate({
      time_now <- format.POSIXct(Sys.time(),"%Y-%m-%d %H:%M:%S UTC")
      msg <- paste0("<h1>",time_now,"</h1>","<h2>Messages with a valid location (last hour):</b>",num_messages(),"</h2>")
      output$connection_status <- renderText(msg)
    })
  })

  observe({
    obs <- values$obs
    if( !is.null(obs)){
      if( nrow(obs) > 0){
        m <- leafletProxy("map") %>% clearGroup("obs") %>%
                  addCircles(lat = obs$latitude, lng = obs$longitude, radius = 25*1E3, stroke=TRUE,
                                   weight=1, color="black", fillColor = "blue", fillOpacity = 0.5, group="obs") %>%
                  addCircles(lat = obs$latitude, lng = obs$longitude, radius = 100*1E3, color="black", weight=0.5,
                             fill = TRUE, fillColor="blue", fillOpacity=0.1, group="obs")
      }
    }
  })

}
# Run the application
shinyApp(ui, server)
