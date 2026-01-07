#!/usr/bin/env Rscript
library(RJDBC)
library(DBI)
library(dplyr)
library(stringi)
library(tibble)
library(bit64)

# readRenviron("~/.Renviron")
# dotenv:::load_dot_env()

options(java.parameters = "-Xmx8000m")

username <- Sys.getenv('USER')
password <- Sys.getenv('PASS')
host <- Sys.getenv('HOST')
port <- Sys.getenv('PORT', unset = 1521)
database <- Sys.getenv('DATABASE')

schema <- Sys.getenv("WH_SCHEMA", unset = "Reporting")
tables <- Sys.getenv('TABLES') # List/Dict
tables <- unlist(strsplit(tables, ","))

wh_host <- Sys.getenv('WH_HOST')
wh_db <- Sys.getenv('WH_DB')
wh_user <- Sys.getenv('WH_USER')
wh_pass <- Sys.getenv('WH_PASS')

# Build the driver using JDBC
jdbcDriver <- JDBC(driverClass="oracle.jdbc.OracleDriver", classPath="/lib/ojdbc6.jar")
# jdbcDriver <- JDBC(driverClass="oracle.jdbc.OracleDriver", classPath="C:/Users/T112332/AppData/Roaming/DBeaverData/drivers/maven/maven-central/com.oracle.database.jdbc/ojdbc8-12.2.0.1.jar")

con <- dbConnect(jdbcDriver, paste0("jdbc:oracle:thin:@//", host, ":", port, "/", database), username, password)
# DB Connection String
wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = wh_db, UID = wh_user, pwd = wh_pass)
# wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = wh_db, Trusted_Connection = 'yes')

# Loop for multiple tables
for (table in tables) {
  # Read Table from Warehouse
  temp <- dbReadTable(wh_con, Id(schema = schema, table = table)) %>%
    mutate_if(is.integer64, as.integer)
  
  #Rename for Oracle
  oracle_table <- gsub("_V$|_C$|_G$", "", table)
  
  # Clean Col names for Oracle
  colnames(temp) <- gsub("\\.", "", colnames(temp))
  
  exists <- RJDBC::dbGetQuery(con, paste0("SELECT table_name FROM user_tables WHERE table_name = '", oracle_table, "'"))
  
  if (nrow(exists) > 0 ) {
    RJDBC::dbSendUpdate(con, paste0("DROP TABLE ", username, '."', oracle_table, '"'))
  }

  # Write Table to Oracle DB
  RJDBC::dbWriteTable(con, Id(schema = username, table = oracle_table), temp)
  
  gc()
}

# Disconnect from DBs
dbDisconnect(wh_con)
dbDisconnect(con)