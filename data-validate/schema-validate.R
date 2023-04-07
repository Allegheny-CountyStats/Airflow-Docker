#!/usr/bin/env Rscript
require(DBI)
require(dplyr)
require(jsonlite)
require(lubridate)

dev <- Sys.getenv('DEV', "no")

if (dev == "yes") {
  dotenv::load_dot_env()
}

source <- Sys.getenv("SOURCE")
dept <- Sys.getenv("DEPT")
table <- Sys.getenv('TABLE')

# Schema read
schema  <- Sys.getenv('COL_SCHEMA')
if (dev == "yes") {
  expected <- fromJSON(paste0("./schemas/", table, "_schema.json"))
} else {
  expected <- fromJSON(schema)
}

wh_host <- Sys.getenv('WH_HOST')
wh_db <- Sys.getenv('WH_DB')
sa_user <- Sys.getenv('SA_USER')
sa_pass <- Sys.getenv('SA_PASS')
wh_user <- Sys.getenv('WH_USER', sa_user)
wh_pass <- Sys.getenv('WH_PASS', sa_pass)

cols <- Sys.getenv("COLS")

# Connection to Warehouse
wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = wh_db, UID = wh_user, pwd = wh_pass)
# Build table name
table_name <- paste(dept, source, table, sep = "_")
# Col types Query
sql <- paste0("SELECT COLUMN_NAME, DATA_TYPE
FROM ", wh_db, ".INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '", table_name, "' AND TABLE_SCHEMA  = 'Staging'")

# Get types
preload <- dbGetQuery(wh_con, sql) %>% 
  as_tibble()
# Load schema
type_check <- select(expected, COLUMN_NAME, DATA_TYPE) %>%
  as_tibble()
# Check to make sure schema matches whats in Staging
check <- all_equal(preload, type_check)

# Run check conditions
if (!isTRUE(check)) {
  dbDisconnect(wh_con)
  stop(check)
}

# Buid list of nulls
nulls <- filter(expected, !is.na(NAs))
null_list <- nulls$COLUMN_NAME

# Check if there are nulls in any of the columns
for (null in null_list) {
  # Look for Null values
  sql <- paste0("SELECT [", null, "] FROM Staging.", paste(dept, source, table, sep = "_"), ' WHERE [', null, '] IS NULL')
  df <- dbGetQuery(wh_con, sql)
  # Check if query returned any rows
  if (nrow(df) > 0) {
    stop(paste("Unexpected missing value(s) in certain", null,  "column."))
  }
}
dbDisconnect(wh_con)