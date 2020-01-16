#!/usr/bin/env Rscript
require(DBI)
require(dplyr)
require(jsonlite)
require(lubridate)

# require(dotenv)cd
# 
load_dot_env()
# expected <- fromJSON("Device_schema.json")$device_schema

schema  <- Sys.getenv('COL_SCHEMA')
expected <- fromJSON(schema)

database <- Sys.getenv("DATABASE")
dept <- Sys.getenv("DEPT")
table <- Sys.getenv('TABLE')

wh_host <- Sys.getenv('WH_HOST')
wh_db <- Sys.getenv('WH_DB')
sa_user <- Sys.getenv('SA_USER')
sa_pass <- Sys.getenv('SA_PASS')

# Connection to Warehouse
wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = wh_db, UID = sa_user, pwd = sa_pass)

sql <- paste0("SELECT COLUMN_NAME, DATA_TYPE
FROM DataWarehouse.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '", paste(dept, database, table, sep = "_"), "'")

preload <- dbGetQuery(wh_con, sql) %>% 
  as_tibble()

type_check <- select(expected, COLUMN_NAME, DATA_TYPE) %>%
  as_tibble()

if (!all.equal(preload, type_check)) {
  dbDisconnect(wh_con)
  stop("Unexpected column type in dataset")
}

nulls <- filter(expected, !is.na(NAs))

cols <- paste(nulls$COLUMN_NAME, collapse = ", ")

random_sql <- paste0("SELECT TOP 10 PERCENT ", cols, " FROM Preload.", paste(dept, database, table, sep = "_"), ' order by newid()')

random <- dbGetQuery(wh_con, random_sql)
dbDisconnect(wh_con)

random_ver <- random %>% na.omit()

if (!all.equal(random, random_ver)) {
  stop("Unexpected missing values in certain colums.")
}