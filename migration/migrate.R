require(dplyr)
require(DBI)
require(readr)

dotenv::load_dot_env()

wh_host <- Sys.getenv('WH_HOST')
wh_db <- Sys.getenv('WH_DB')
wh_user <- Sys.getenv('WH_USER')
wh_pass <- Sys.getenv('WH_PASS')

# dev_host <- Sys.getenv('dev_HOST')

source <- Sys.getenv('SOURCE')
tables <- Sys.getenv('TABLES')
tables <- unlist(strsplit(tables, ","))
dept <- Sys.getenv('DEPT')

hc_user <- Sys.getenv('HC_USER')
hc_pass <- Sys.getenv('HC_PASS')
hc_db <- Sys.getenv('HC_DB')

# Connection to Warehouse
wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = wh_db, UID = wh_user, pwd = wh_pass)
# dev_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = dev_host, database = wh_db, UID = wh_user, pwd = wh_pass)
hc_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = hc_db, UID = hc_user, pwd = hc_pass)

for (table in tables) {
  full_table <- paste0("Master.", dept, "_", source, "_", table)
  temp <- dbReadTable(wh_con, SQL(full_table))
  
  dbWriteTable(hc_con, SQL(full_table), temp)
}

dbDisconnect(wh_con)
dbDisconnect(hc_con)