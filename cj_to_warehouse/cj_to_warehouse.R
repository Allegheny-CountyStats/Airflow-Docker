require(DBI)
require(dplyr)
require(jsonlite)
require(lubridate)

# dotenv::load_dot_env()

source <- Sys.getenv("SOURCE")
dept <- Sys.getenv("DEPT")
tables <- Sys.getenv('TABLES')
schema <- Sys.getenv('SCHEMA', 'Reporting')
append <- Sys.getenv('APPEND', 'FALSE')
append <- append == "TRUE"

wh_host <- Sys.getenv('WH_HOST')
wh_db <- Sys.getenv('WH_DB')
wh_user <- Sys.getenv('WH_USER')
wh_pass <- Sys.getenv('WH_PASS')

cj_host <- Sys.getenv('CJ_HOST')
cj_db <- Sys.getenv('CJ_DB')
cj_user <- Sys.getenv('CJ_USER')
cj_pass <- Sys.getenv('CJ_PASS')

# Connection to Warehouses
wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = wh_db, UID = wh_user, pwd = wh_pass)
cj_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = cj_host, database = cj_db, UID = cj_user, pwd = cj_pass)

# Get list of Tables
tables <- unlist(strsplit(tables, ","))

# Transfer each Table from CJ (Criminal Justice Data Warehouse) to Dave (Data Warehouse Prime)
for (table in tables) {
  table_name <- paste(schema, paste(dept, source, table, sep = "_"), sep = ".")
  
  temp <- dbReadTable(cj_con, SQL(table_name))
  
  if (append) {
    dbWriteTable(wh_con, SQL(table_name), temp, append = T)
  } else {
    dbWriteTable(wh_con, SQL(table_name), temp, overwrite = T)
  }
}

dbDisconnect(wh_con)
dbDisconnect(cj_con)