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
  new_table <- paste0("Master.", paste(dept, source, table, sep = "_"))
  
  # Skip if No Table to Append with
  if(!dbExistsTable(wh_con, SQL(prel_table))) {
  # Prepare ID's for Removal
  } else if (dbExistsTable(wh_con, SQL(new_table))) {
    id_q <- paste0("SELECT DISTINCT [", id_col, "] FROM ", prel_table, "")
    id_df <- dbGetQuery(wh_con, id_q)
    id_l <-  as.numeric(nrow(id_df))
    
    # Detect new rows within tables required to change/add-new-records [nrow new rows in REQ_TABLES > 0]
    if (table %in% req_tables){
      rc_q <- paste0("SELECT DISTINCT [", id_col, "] FROM ", new_table, "")
      rc_df <- dbGetQuery(wh_con, rc_q)
      if(!nrow(dplyr::anti_join(id_df,rc_df,id_col))>0){
        stop("No row change detected in required tables (defined in REQ_TABLES variable): Check staging
             or spelling of table within variable")
      }
    }
    
    # Iterate through 100 at a time
    if(id_l > 100) {
      id_seq <- seq(from = 1, to = id_l, by = 100)
      for(sub in id_seq) {
        ids_sub <- id_df[[id_col]][sub:min(sub + 99, id_l)]
        ids <- paste(ids_sub, collapse = "', '")
        drop_cmd <- paste0("DELETE FROM ", new_table,
                           " WHERE ", id_col, " IN ('", ids, "')")
        dbExecute(wh_con, drop_cmd)
      }
    # If fewer than 100 Id's do once
    } else {
      ids <- paste(id_df[[id_col]], collapse = "', '")
      drop_cmd <- paste0("DELETE FROM ", new_table,
                         " WHERE ", id_col, " IN ('", ids, "')")
      dbExecute(wh_con, drop_cmd)
    }
    
    cols <- paste0("SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '", table_name, "' AND TABLE_SCHEMA = 'Staging'")
    col_names <- dbGetQuery(wh_con, cols)$COLUMN_NAME %>%
      paste(collapse = "], [")
    
    # Append to Master Table
    sql_insert <- paste("WITH NewData (AS SELECT * FROM", prel_table, ")
                        INSERT INTO", new_table, "(", col_names, ") SELECT * FROM NewData;")
    dbExecute(wh_con, sql_insert)
    
    # Drop Stagging Table
    sql_drop <- paste('DROP TABLE IF EXISTS', prel_table)
    dbExecute(wh_con, sql_drop)
  } else {
    sql_move <- paste("ALTER SCHEMA Master TRANSFER", prel_table)
    dbExecute(wh_con, sql_move)
  }
}

# Disconnect from Warehouse
dbDisconnect(wh_con)