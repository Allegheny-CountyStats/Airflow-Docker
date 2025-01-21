#!/usr/bin/env Rscript
require(DBI)
require(dplyr)

# dotenv::load_dot_env()

dept <- Sys.getenv("DEPT")
table <- Sys.getenv('TABLE')
tables <- Sys.getenv('TABLES', table)
tables <- unlist(strsplit(tables, ","))
req_tables <- Sys.getenv('REQ_TABLES')
req_tables <- unlist(strsplit(req_tables, ","))
calculated_uid <- Sys.getenv('CALC_UID', "NO")

target_schema <- Sys.getenv('TARGET_SCHEMA', "Master")

id_col <- Sys.getenv("ID_COL")
source <- Sys.getenv('SOURCE')

wh_host <- Sys.getenv('WH_HOST')
wh_db <- Sys.getenv('WH_DB')
wh_user <- Sys.getenv('WH_USER')
wh_pass <- Sys.getenv('WH_PASS')

wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = wh_db, UID = wh_user, pwd = wh_pass)

# Run Once for each table
for (table in tables) {
  # Get Preload Table
  table_name <-  paste(dept, source, table, sep = "_")
  prel_table <- paste0("Staging.", table_name)
  new_table <- paste0(target_schema, ".", paste(dept, source, table, sep = "_"))
  
  # Skip if No Table to Append with
  if(dbExistsTable(wh_con, DBI::Id(schema = "Master", table = table_name))) {
    # Delete rows
    sql_insert <- paste0("
    DELETE m
    FROM ", new_table, " m
    INNER JOIN ", prel_table, " s ON m.", id_col," = s.", id_col, ";")
    x <- dbExecute(wh_con, sql_insert)
    print(paste0(x, " rows matched with ", prel_table," and then deleted from ", new_table))
    
    # Gather column names
    cols <- paste0("SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '", table_name, "' AND TABLE_SCHEMA = 'Staging'")
    col_list <- col_names <- dbGetQuery(wh_con, cols)$COLUMN_NAME 
    if(calculated_uid == "YES"){
      cols <- paste0("SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '", table_name, "' AND TABLE_SCHEMA = 'Staging' AND COLUMN_NAME <> '", id_col, "'")
      col_names <- dbGetQuery(wh_con, cols)$COLUMN_NAME %>%
        paste(collapse = "], [")
    }
    
    # Append to Master Table 
    y <- dbExecute(wh_con, sql_insert)
    
    if(y-x < 0){
      paste("Difference of", x-y, "rows deleted within master without replacement/update from staging")
    }else{
      print(paste(y, "records added back to", new_table))
    }
    
    # Drop Staging Table
    sql_drop <- paste('DROP TABLE IF EXISTS', prel_table)
    dbExecute(wh_con, sql_drop)
  } else {
    sql_move <- paste("ALTER SCHEMA", target_schema, "TRANSFER", prel_table)
    dbExecute(wh_con, sql_move)
  }
}

# Disconnect from Warehouse
dbDisconnect(wh_con)