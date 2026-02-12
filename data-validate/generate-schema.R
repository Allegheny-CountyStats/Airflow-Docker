require(DBI)
require(dplyr)
require(jsonlite)
require(httr)

dotenv::load_dot_env()

database <- Sys.getenv('DATABASE')
database <- gsub("\\.COUNTY\\.ALLEGHENY\\.LOCAL", "", database)
source <- Sys.getenv("SOURCE", unset = database)

dept <- Sys.getenv("DEPT")
schema <- Sys.getenv("SCHEMA", "Staging")

wh_host <- Sys.getenv('WH_HOST')
wh_db <- Sys.getenv('WH_DB')
wh_user <- Sys.getenv('WH_USER')
wh_pass <- Sys.getenv('WH_PASS')
na_false <- Sys.getenv('NA_FALSE')

na_list <- unlist(strsplit(na_false, ","))

table <- Sys.getenv("TABLE")

sql <- paste0("SELECT COLUMN_NAME, DATA_TYPE
FROM ", wh_db, ".INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '", paste(dept, source, table, sep = "_"), "' AND TABLE_SCHEMA = '", schema, "'")

wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = wh_db, Trusted_Connection= "yes")

preload <- dbGetQuery(wh_con, sql) %>%
  mutate(NAs = case_when(COLUMN_NAME %in% na_list ~ FALSE,
                    TRUE ~ NA))
dbDisconnect(wh_con)

schema_name <- paste(table, "schema", sep = "_")
filename <- paste0("./schemas/", schema_name, ".json")

# Create Folder if its not there
if (!dir.exists("./schemas")) {
  dir.create("./schemas")
}

# Generate Schema
write_json(preload, filename, unbox = TRUE)

print(schema_name)

# Verify the file
file.edit(filename)

# Airflow
airflow_user <- Sys.getenv('AIRFLOW_USER')
airflow_password <- Sys.getenv('AIRFLOW_PASSWORD')

if(airflow_user != ""){
  #To Dev
  json_body <- jsonlite::toJSON(list(username = airflow_user, password = airflow_password), auto_unbox = TRUE)
  response <- httr::POST(
    "https://devairflow.alleghenycounty.us:8080/auth/token",
    httr::add_headers(
      "Content-Type" = "application/json"
    ),
    body = json_body, encode = "raw"
    )
  stop_for_status(response)
  raise <- suppressMessages(httr::content(response, "text"))
  api_token <- jsonlite::fromJSON(raise)
  
  json_body <- jsonlite::toJSON(list(key = schema_name, value = jsonlite::toJSON(preload)), auto_unbox = TRUE)
  response <- httr::POST(
    "https://devairflow.alleghenycounty.us:8080/api/v2/variables",
    httr::add_headers(
      "Content-Type" = "application/json",
      "Authorization" = paste0("Bearer ", api_token$access_token)
    ),
    body = json_body, encode = "raw")
  stop_for_status(response)
  
  #To Prod
  json_body <- jsonlite::toJSON(list(username = airflow_user, password = airflow_password), auto_unbox = TRUE)
  response <- httr::POST(
    "https://airflow.alleghenycounty.us:8080/auth/token",
    httr::add_headers(
      "Content-Type" = "application/json"
    ),
    body = json_body, encode = "raw"
  )
  stop_for_status(response)
  raise <- suppressMessages(httr::content(response, "text"))
  api_token <- jsonlite::fromJSON(raise)
  
  json_body <- jsonlite::toJSON(list(key = schema_name, value = jsonlite::toJSON(preload)), auto_unbox = TRUE)
  response <- httr::POST(
    "https://airflow.alleghenycounty.us:8080/api/v2/variables",
    httr::add_headers(
      "Content-Type" = "application/json",
      "Authorization" = paste0("Bearer ", api_token$access_token)
    ),
    body = json_body, encode = "raw")
  stop_for_status(response)
}

