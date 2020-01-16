require(DBI)
require(dplyr)
require(dotenv)

load_dot_env()

expected  <- Sys.getenv('COL_SCHEMA')

database <- Sys.getenv("DATABASE")
dept <- Sys.getenv("DEPT")

wh_host <- Sys.getenv('WH_HOST')
wh_db <- Sys.getenv('WH_DB')
sa_user <- Sys.getenv('SA_USER')
sa_pass <- Sys.getenv('SA_PASS')

wh_host <- "DEVSQL17.County.Allegheny.Local\\CountyStat_DW"
wh_database <- 'DataWarehouse'

wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = wh_db, UID = sa_user, pwd = sa_pass)

table <- "Store"

sql <- paste0("SELECT COLUMN_NAME, DATA_TYPE
FROM DataWarehouse.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '", paste(dept, database, table, sep = "_"), "'")

preload <- dbGetQuery(wh_con, sql)

schema_name <- paste(table, "schema", sep = "_")
filename <- paste0(schema_name, ".json")

# Generate Schema
schema <- dbGetQuery(wh_con, sql)
write_json(schema, filename, unbox = TRUE)

file.edit(filename)

dbDisconnect(wh_con)
