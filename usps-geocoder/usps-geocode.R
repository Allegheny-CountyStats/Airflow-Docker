require(sp)
require(rgdal)
require(DBI)
require(sf)

# dotenv::load_dot_env()

source("usps_geocoder.R")

wh_host <- Sys.getenv('WH_HOST')
wh_db <- Sys.getenv('WH_DB')
wh_user <- Sys.getenv('WH_USER')
wh_pass <- Sys.getenv('WH_PASS')

schema <- Sys.getenv('schema', 'Master')

dept <- Sys.getenv('DEPT')
source <- Sys.getenv('SOURCE')
table <- Sys.getenv('TABLE')

g_table <- paste(dept, source, table, "G", sep = "_")
source_table <- paste(dept, source, table, sep = "_")
u_table <- paste0(schema, ".", source_table, "_uspsG")
id <- Sys.getenv('ID_COL')
where <- Sys.getenv('WHERE')
where_stmnt <- ifelse(where == "", "", paste(" AND ", where))

# DB Connection String
wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = wh_db, UID = wh_user, pwd = wh_pass)

# Build query
g_query <- paste0("SELECT DISTINCT ", id, ", FULL_ADDRESS FROM ", schema, ".", g_table, " g
                  WHERE g.latitude IS NULL", where_stmnt)

# Get Failed geocoded addresses
fails <- dbGetQuery(wh_con, g_query) %>%
  mutate_if(is.character, iconv, to = "UTF-8")

if (dbExistsTable(wh_con, SQL(u_table))) {
  # Grab Past geocoded addresses
  u_past <- dbGetQuery(wh_con, paste("SELECT * FROM", u_table))
  
  # Remove previous geocode attempts
  new_fails <- fails[which(!(u_past[[id]] %in% fails[[id]])),]
} else {
  new_fails <- fails
}

# If there are new addresses, geocode them
if (nrow(new_fails) > 0) {
  usps <- new_fails %>%
    mutate_uspsGeo(FULL_ADDRESS)
  
  usps_sp <- usps %>%
    filter(!is.na(longitude)) %>%
    st_as_sf(coords = c("longitude", "latitude"),
             crs = 4326) %>%
    as_Spatial()
  
  counties <- readOGR("us_counties.geojson")
  
  results <- over(usps_sp, counties)
  
  usps_sp$STATE <- results$STATE_NAME
  usps_sp$COUNTY <- results$NAME
  
  usps_county <- left_join(usps, usps_sp@data, by = c(id, "FULL_ADDRESS")) 
  
  if (dbExistsTable(wh_con, SQL(u_table))) {
    dbWriteTable(wh_con, SQL(u_table), usps_county, append = TRUE)
  } else {
    dbWriteTable(wh_con, SQL(u_table), usps_county)
  }
}

dbDisconnect(wh_con)