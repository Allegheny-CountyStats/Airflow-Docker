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
suffix_name <- Sys.getenv('WHB_SUFFIX', NA)

# Trusted (Kerberos) connections use the ticket cache mounted into the container at /tmp/krb5cc_0
is_yes <- function(x) tolower(x) %in% c("yes", "true", "1")
wha_trusted <- is_yes(Sys.getenv('WHA_TRUSTED', 'No'))
whb_trusted <- is_yes(Sys.getenv('WHB_TRUSTED', 'No'))

# Optional SQL run on Warehouse B before the transfer, and a query run after it whose first row is printed as the
# final log line (DockerOperator pushes that line to XCom)
whb_sql_before <- Sys.getenv('WHB_SQL_BEFORE')
whb_query <- Sys.getenv('WHB_QUERY')

connect <- function(host, db, user, pass, trusted) {
  if (trusted) {
    dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = host, database = db, Trusted_Connection = "Yes")
  } else {
    dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = host, database = db, UID = user, pwd = pass)
  }
}

# Connection to Warehouse B
whb_con <- connect(whb_host, whb_db, whb_user, whb_pass, whb_trusted)

if (whb_sql_before != "") {
  dbExecute(whb_con, whb_sql_before)
}

# Get list of Tables (may be empty for SQL-only runs)
tables <- unlist(strsplit(tables, ","))

if (length(tables) > 0) {
  # Connection to Warehouse A
  wha_con <- connect(wha_host, wha_db, wha_user, wha_pass, wha_trusted)

  # Transfer each Table from Warehouse A to Warehouse B
  for (table in tables) {
    if (wha_db == "Shiny") {
      table_name <- table
    } else {
      table_name <- paste(dept, source, table, sep = "_")
    }

    if(!is.na(suffix_name)){
      new_table <- paste(dept, source, table, suffix_name, sep = "_")
    }else{
      new_table <- paste(dept, source, table, sep = "_")
    }

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
}

if (whb_query != "") {
  result <- dbGetQuery(whb_con, whb_query)
  print(result)
  cat(paste(names(result), result[1, ], sep = "=", collapse = " "), "\n")
}

dbDisconnect(whb_con)
