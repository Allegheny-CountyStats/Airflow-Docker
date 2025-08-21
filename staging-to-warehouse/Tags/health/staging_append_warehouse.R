#!/usr/bin/env Rscript
require(DBI)
require(dplyr)

# dotenv::load_dot_env()

dept <- Sys.getenv("DEPT")
tables <- Sys.getenv('TABLES') # List/Dict
tables <- unlist(strsplit(tables, ","))

target_schema <- Sys.getenv('TARGET_SCHEMA', "Master")

source <- Sys.getenv('SOURCE')

wh_host <- Sys.getenv('WH_HOST')
wh_db <- Sys.getenv('WH_DB')
wh_user <- Sys.getenv('WH_USER')
wh_pass <- Sys.getenv('WH_PASS')

wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = wh_db, UID = wh_user, pwd = wh_pass)

for (table in tables) {
  # Get Preload Table
  table_name <-  paste(dept, source, table, sep = "_")
  prel_table <- paste0("Staging.", table_name)
  new_table <- paste0(target_schema, ".", paste(dept, source, table, sep = "_"))
  
  if (dbExistsTable(wh_con, SQL(new_table))) {
    cols <- paste0("SELECT COLUMN_NAME
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_NAME = '", table_name, "' AND TABLE_SCHEMA = 'Staging'
  ORDER BY ORDINAL_POSITION 
                   ")
    col_names <- dbGetQuery(wh_con, cols)$COLUMN_NAME %>%
      paste(collapse = "], [")
    
    # Append to Master Table
    sql_insert <- paste0("WITH NewData AS (SELECT * FROM ", prel_table, ")
                        INSERT INTO ", new_table, " ([", col_names, "]) SELECT * FROM NewData")
    y <- dbExecute(wh_con, sql_insert)
    print(paste(y, "records added to", new_table))
    
    # Drop Stagging Table
    sql_drop <- paste('DROP TABLE IF EXISTS', prel_table)
    dbExecute(wh_con, sql_drop)
  } else {
    sql_move <- paste("ALTER SCHEMA", target_schema, "TRANSFER", prel_table)
    dbExecute(wh_con, sql_move)
  }
}

dbDisconnect(wh_con)