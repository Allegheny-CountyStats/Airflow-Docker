#!/usr/bin/env Rscript
require(DBI)
require(dplyr)

# dotenv::load_dot_env()

wh_host <- Sys.getenv('WH_HOST')
wh_db <- Sys.getenv('WH_DB')
wh_user <- Sys.getenv('WH_USER')
wh_pass <- Sys.getenv('WH_PASS')

schema <- Sys.getenv('schema', 'Master')

source_table <- Sys.getenv('TABLE')
t_cols <- Sys.getenv('T_COLS')
t_cols <- unlist(strsplit(t_cols, ","))
t_cols <- paste0("t.", paste(t_cols, collapse = ", t."))
g_cols <- Sys.getenv('G_COLS')
g_cols <- unlist(strsplit(g_cols, ","))
g_cols <- paste0("g.", paste(g_cols, collapse = ", g."))
cols <- paste(t_cols, g_cols, sep = ", ")
g_table <- paste0(source_table, "_G")
id <- Sys.getenv('ID')
where <- Sys.getenv('WHERE')
where_stmnt <- ifelse(where == "", "", paste(" AND ", where))

# DB Connection String
wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = wh_db, UID = wh_user, pwd = wh_pass)

g_query <- paste0("SELECT ", cols, " FROM ", schema, ".", g_table, " g
                  LEFT JOIN ", schema, ".", source_table, " t
	                  ON g.", id, " = t.", id, "
                  WHERE g.latitude IS NULL", where_stmnt)
fails <- dbGetQuery(wh_con, g_query)

write.csv(fails, "FOOD_FACILITY_FAILURES.csv", row.names = F)
