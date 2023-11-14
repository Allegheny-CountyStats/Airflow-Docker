#!/usr/bin/env Rscript
library(RJDBC)
library(DBI)
library(dplyr)
library(stringi)
library(tibble)

# dotenv:::load_dot_env()

options(java.parameters = "-Xmx8000m")

username <- Sys.getenv('USER')
password <- Sys.getenv('PASS')
host <- Sys.getenv('HOST')
port <- Sys.getenv('PORT', unset = 1521)
database <- Sys.getenv('DATABASE')
dept <- Sys.getenv("DEPT")
schema <- Sys.getenv("SCHEMA")
tables <- Sys.getenv('TABLES') # List/Dict

wh_host <- Sys.getenv('WH_HOST')
wh_db <- Sys.getenv('WH_DB')
sa_user <- Sys.getenv('SA_USER')
sa_pass <- Sys.getenv('SA_PASS')
wh_user <- Sys.getenv('WH_USER', unset = sa_user)
wh_pass <- Sys.getenv('WH_PASS', unset = sa_pass)

max_cols_load <- Sys.getenv("MAX_COLS")
max_cols <- unlist(strsplit(max_cols_load, ","))

sql <- Sys.getenv("SQL")

append_col <- Sys.getenv('APPEND_COL')
append_type <- Sys.getenv('APPEND_TYPE', unset = "MAX")
append_sign <- Sys.getenv('APPEND_SIGN', unset = ">")

# Build the driver using JDBC
system("kinit sa00427@COUNTY.ALLEGHENY.LOCAL -k -t Kerberos/sa00427.keytab")
Sys.sleep(2)
jdbcDriver <- JDBC(driverClass="oracle.jdbc.OracleDriver", classPath="/lib/ojdbc6.jar")
# jdbcDriver <- JDBC(driverClass="oracle.jdbc.OracleDriver", classPath="C:/Users/T112332/AppData/Roaming/DBeaverData/drivers/maven/maven-central/com.oracle.database.jdbc/ojdbc8-12.2.0.1.jar")

con <- dbConnect(jdbcDriver, paste0("jdbc:oracle:thin:@//", host, ":", port, "/", database), username, password)
# DB Connection String
wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = wh_db, Trusted_Connection = "Yes", TrustServerCertificate = "yes")

tables <- unlist(strsplit(tables, ","))

# Rename Database for SQL Server
prel_db <- gsub("\\.COUNTY\\.ALLEGHENY\\.LOCAL", "", database)

for (table in tables) {
  options(warn=-1)
  if (schema != '') {
    query <- paste("SELECT * FROM", paste(schema, table, sep = "."))
  } else if (sql == '') {
    query <- paste("SELECT * FROM", table)
  } else {
    query <- sql
  }
  
  # Append Value and Query
  if (append_col != '') { 
    value_sql <- paste0("SELECT ", append_type, "(", append_col, ") as value FROM Master.", paste(dept, database, table, sep = "_"))
    value <- dbGetQuery(wh_con, value_sql)$value
    if (sql != '') { 
      query <- gsub("VALUE", value, query)
    } else {
      query <- paste0(query, " WHERE ", append_col, " ", append_sign, " '", value, "'")
    }
  }
  
  # Sequence Fetches for large datasets
  res <- dbSendQuery(con, query)
  result <- list()
  i <- 1
  result[[i]] <- fetch(res, n = 20000)
  while(nrow(chunk <- fetch(res, n = 20000)) > 0) {
    i <- i + 1
    result[[i]] <- chunk
    gc()
  }
  gc()
  # Mind Rows and re-encode cols to UTF-8
  temp <- do.call(rbind, result) %>%
    mutate_if(is.character, .funs = function(x) stri_encode(x, "", "UTF-8"))
  
  # Remove Spaces from Col Names
  colnames(temp) <- gsub(pattern = " ", "_", colnames(temp))
  
  if (max_cols_load == "") { 
    dbWriteTable(wh_con, SQL(paste("Staging", paste(dept, prel_db, table, sep = "_"), sep =".")), temp, overwrite = TRUE)
    } else {
    if (any(max_cols %in% colnames(temp))) {
      # ID Max Cols for this table
      cols <- colnames(temp)[which(colnames(temp) %in% max_cols)]
      
      # Create Max Cols Type List
      types <- data.frame(cols = cols) %>%
        mutate(names = "varchar(max)") %>%
        deframe() %>%
        as.list()
      
      # Move Max columns to end of table
      temp <- select(temp, c(-cols, everything()))
      
      # Transfer Data to Warehouse
      dbWriteTable(wh_con, SQL(paste("Staging", paste(dept, prel_db, table, sep = "_"), sep =".")), temp, field.type = types, overwrite = TRUE)
    } else {
      dbWriteTable(wh_con, SQL(paste("Staging", paste(dept, prel_db, table, sep = "_"), sep =".")), temp, overwrite = TRUE)
    }
  } 
  options(warn=0)
}

dbDisconnect(con)
dbDisconnect(wh_con)
 