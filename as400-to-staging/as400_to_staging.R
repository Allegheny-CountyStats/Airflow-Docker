#!/usr/bin/env Rscript
library(RJDBC)
library(dplyr)
library(rlang)
# Load Credentials
# dotenv::load_dot_env()

dept <- Sys.getenv("dept")
host <- Sys.getenv("host")
uid <- Sys.getenv("uid")
pwd <- Sys.getenv("pwd")
source <- Sys.getenv("source")
schema <- Sys.getenv("schema")
table <- Sys.getenv('table')
sql <- Sys.getenv("sql")

wh_host <- Sys.getenv('wh_host')
wh_db <- Sys.getenv('wh_db')
wh_user <- Sys.getenv('wh_user')
wh_pass <- Sys.getenv('wh_pass')
append_col <- Sys.getenv('append_col')
append_type <- Sys.getenv('append_type', "MAX")
append_sign <- Sys.getenv('append_sign', ">")

date_col <- Sys.getenv('date_col', append_col)

# Build Driver
# jdbcDriver <- JDBC(driverClass="com.ibm.as400.access.AS400JDBCDriver", classPath="C:/Users/T112332/Drivers/jt400-jdk9-10.7.jar")
jdbcDriver <- JDBC(driverClass="com.ibm.as400.access.AS400JDBCDriver", classPath="/lib/jt400-jdk9-10.1.jar")

# Connections
ibm <- dbConnect(jdbcDriver, host, user = uid, password = pwd)
# DB Connection String
wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = wh_db, UID = wh_user, pwd = wh_pass)

# Import SQL
if (sql != '') {
  query <- sql
} else {
  query <- paste0("SELECT * FROM ", schema, ".", table)
}

# Append Value and Query
if (append_col != '') { 
  value_sql <- paste0("SELECT ", append_type, "(", append_col, ") as value FROM Master.", paste(dept, source, table, sep = "_"))
  value <- dbGetQuery(wh_con, value_sql)$value
  if (sql != '') { 
    query <- gsub("VALUE", value, query)
  } else {
    query <- paste0(query, " WHERE ", append_col, " ", append_sign, " '", value, "'")
  }
}

temp <- dbGetQuery(ibm, query) 

if (date_col != "") {
  temp <- temp %>%
    mutate(date = as.Date(as.character(!!sym(date_col)), "1%y%j"))
}

table_name <- paste(dept, source, table, sep = "_")

dbWriteTable(wh_con, SQL(paste0("Staging.", table_name)), temp, overwrite= TRUE)

dbDisconnect(ibm)
dbDisconnect(wh_con)