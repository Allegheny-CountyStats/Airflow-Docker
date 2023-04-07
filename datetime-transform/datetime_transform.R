#!/usr/bin/env Rscript
library(DBI)

# dotenv:::load_dot_env()

database <- Sys.getenv('DATABASE')
source <- Sys.getenv("SOURCE", unset = database)
dept <- Sys.getenv("DEPT")
table <- Sys.getenv('TABLE') # List/Dict
cols <- Sys.getenv("COLS")

cols <- unlist(strsplit(cols, ","))

format <- Sys.getenv("FORMAT", unset = 121)

wh_host <- Sys.getenv('WH_HOST')
wh_db <- Sys.getenv('WH_DB')
wh_user <- Sys.getenv('WH_USER')
wh_pass <- Sys.getenv('WH_PASS')

full_table <- paste(dept, source, table, sep = "_")

wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = wh_db, UID = wh_user, pwd = wh_pass)

# Try Date Conversion for each Column
for (col in cols) { 
  sql <- paste0("UPDATE Staging.", full_table, " SET ", col, " = TRY_CONVERT(datetime, ", col, ", ", format, ")")
 dbExecute(wh_con, sql)
}

# Alter each column to datetime
for (col in cols) {
  sql <- paste0("ALTER TABLE Staging.", full_table, "
                  ALTER COLUMN ", col, " datetime;")
  dbExecute(wh_con, sql)
}

dbDisconnect(wh_con)