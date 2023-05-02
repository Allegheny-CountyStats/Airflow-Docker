#!/usr/bin/env Rscript
require(DBI)

# dotenv::load_dot_env()

dept <- Sys.getenv("DEPT")
tables <- Sys.getenv('TABLES') # List/Dict
tables <- unlist(strsplit(tables, ","))
req_tables <- Sys.getenv('REQ_TABLES')
req_tables <- unlist(strsplit(req_tables, ","))

source <- Sys.getenv('SOURCE')
id_col <- Sys.getenv('ID_COL')

wh_host <- Sys.getenv('WH_HOST')
wh_db <- Sys.getenv('WH_DB')
wh_user <- Sys.getenv('WH_USER')
wh_pass <- Sys.getenv('WH_PASS')

wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = wh_db, UID = wh_user, pwd = wh_pass)

for (table in tables) {
  # Get Preload Table
  prel_table <- paste0("Staging.", paste(dept, source, table, sep = "_"))
  new_table <- paste0("Master.", paste(dept, source, table, sep = "_"))
  
  # Check to see if table has already been moved in previous run
  if (dbExistsTable(wh_con, SQL(prel_table))) {
    id_q <- paste0("SELECT DISTINCT [", id_col, "] FROM ", prel_table, "")
    id_df <- dbGetQuery(wh_con, id_q)
    id_l <-  as.numeric(nrow(id_df))
    
    if (table %in% req_tables){
      rc_q <- paste0("SELECT DISTINCT [", id_col, "] FROM ", new_table, "")
      rc_df <- dbGetQuery(wh_con, rc_q)
      if(!nrow(dplyr::anti_join(id_df,rc_df,id_col))>0){
        stop("No row change detected in required tables (defined in REQ_TABLES variable): Check staging
             or spelling of table within variable")
      }
    }
    # Drop Old Table
    sql_drop <- paste('DROP TABLE IF EXISTS', new_table)
    dbExecute(wh_con, sql_drop)
    # Move to Finalized Schema
    sql_move <- paste("ALTER SCHEMA Master TRANSFER", prel_table)
    dbExecute(wh_con, sql_move)
  } else {
    warning(paste("Skipped transfer of ", prel_table, " to Master, table is missing."))
  }
}

dbDisconnect(wh_con)