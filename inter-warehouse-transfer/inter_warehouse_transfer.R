require(DBI)
require(dplyr)
require(jsonlite)
require(lubridate)
require(tibble)

# dotenv::load_dot_env()

source <- Sys.getenv("SOURCE")
dept <- Sys.getenv("DEPT")
tables <- Sys.getenv('TABLES')
schema <- Sys.getenv('SCHEMA', 'Reporting')
schema_b <- Sys.getenv('SCHEMA_B', schema)

max_cols_load <- Sys.getenv("MAX_COLS")
max_cols <- unlist(strsplit(max_cols_load, ","))

wha_host <- Sys.getenv('WHA_HOST')
wha_db <- Sys.getenv('WHA_DB')
wha_user <- Sys.getenv('WHA_USER')
wha_pass <- Sys.getenv('WHA_PASS')

whb_host <- Sys.getenv('WHB_HOST')
whb_db <- Sys.getenv('WHB_DB')
whb_user <- Sys.getenv('WHB_USER')
whb_pass <- Sys.getenv('WHB_PASS')

# Connection to Warehouses
wha_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wha_host, database = wha_db, UID = wha_user, pwd = wha_pass)
whb_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = whb_host, database = whb_db, UID = whb_user, pwd = whb_pass)

# Get list of Tables
tables <- unlist(strsplit(tables, ","))

# Transfer each Table from Warehouse A to Warehouse B
for (table in tables) {
  if (wha_db == "Shiny") {
    table_name <- table
  } else {
    table_name <- paste(dept, source, table, sep = "_")
  }
  
  new_table <- paste(dept, source, table, sep = "_")
  
  temp <- dbReadTable(wha_con, Id(schema = schema , table = table_name))
  
  if (max_cols_load == "") { 
    dbWriteTable(whb_con, Id(schema = schema_b, table = new_table), temp, overwrite = TRUE)
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
      temp <- select(temp, c(-all_of(cols), everything()))
      
      # Transfer Data to Warehouse
      dbWriteTable(whb_con, Id(schema = schema_b, table = new_table), temp, field.type = types, overwrite = TRUE)
    } else {
      dbWriteTable(whb_con, Id(schema = schema_b, table = new_table), temp, overwrite = TRUE)
    }
  }
  print(paste(table, "transferred"))
  gc()
}

dbDisconnect(wha_con)
dbDisconnect(whb_con)