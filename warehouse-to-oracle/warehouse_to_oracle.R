#!/usr/bin/env Rscript
library(RJDBC)
library(DBI)
library(dplyr)
library(stringi)
library(tibble)
library(lubridate)

# dotenv:::load_dot_env()

username <- Sys.getenv('USER')
password <- Sys.getenv('PASS')
host <- Sys.getenv('HOST')
port <- Sys.getenv('PORT', unset = 1521)
database <- Sys.getenv('DATABASE')
schema <- Sys.getenv("SCHEMA")

wh_host <- Sys.getenv('WH_HOST')
wh_db <- Sys.getenv('WH_DB')
wh_user <- Sys.getenv('WH_USER')
wh_pass <- Sys.getenv('WH_PASS')

wh_schema <- Sys.getenv("WH_SCHEMA", unset = "Reporting")

dept <- Sys.getenv("DEPT")
source <- Sys.getenv("SOURCE")
tables <- Sys.getenv('TABLES') # List/Dict
tables <- unlist(strsplit(tables, ","))

# Build the driver using JDBC
jdbcDriver <- JDBC(driverClass="oracle.jdbc.OracleDriver", classPath="/lib/ojdbc6.jar")
# jdbcDriver <- JDBC(driverClass="oracle.jdbc.OracleDriver", classPath="C:/Users/T112332/AppData/Roaming/DBeaverData/drivers/maven/maven-central/com.oracle.database.jdbc/ojdbc8-12.2.0.1.jar")

con <- dbConnect(jdbcDriver, paste0("jdbc:oracle:thin:@//", host, ":", port, "/", database), username, password)
# DB Connection String
wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = wh_db, UID = wh_user, pwd = wh_pass)

for (table in tables) {
  table_full <- paste0(wh_schema, ".", dept, "_", source, "_", table)
  target_table <- paste0(username, ".", toupper(table))
  
  temp <- dbReadTable(wh_con, SQL(table_full))
  
  # Clean Col names for Oracle
  colnames(temp) <- toupper(colnames(temp))
  colnames(temp) <- gsub("\\.", "", colnames(temp))
  
  print(colnames(temp))
  print(target_table)
  
  tabs <- dbGetQuery(con, 'SELECT * FROM USER_TABLES')
  if (toupper(table) %in% tabs$TABLE_NAME) {
    dbRemoveTable(con, SQL(target_table))
  }
  
  dbWriteTable(con, name = SQL(target_table), temp, rownames=FALSE)
}

dbDisconnect(con)
dbDisconnect(wh_con)