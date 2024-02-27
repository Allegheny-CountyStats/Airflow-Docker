#!/usr/bin/env Rscript
require(httr) # http rest requests
require(jsonlite) # fromJSON
require(utils) # URLencode functions
require(DBI)
require(dplyr)

options(scipen = 999)

offset_amount <- as.numeric(Sys.getenv('offset_amount', 2500))

sql_statement <- Sys.getenv("sql_statement")
resource_code <- Sys.getenv("resource_code")

table_name <- Sys.getenv("table_name")

wh_host <- Sys.getenv('wh_host')
wh_db <- Sys.getenv('wh_db')
wh_user <- Sys.getenv('wh_user')
wh_pass <- Sys.getenv('wh_pass')

# DB Connection String
wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = wh_db, UID = wh_user, pwd = wh_pass)

# Get Data FROM API
if (nchar(sql_statement) > 10){
  url_string <- paste0("https://data.wprdc.org/api/action/datastore_search_sql?sql=",sql_statement)
  # Encode URL
  url <- URLencode(url_string, repeated = TRUE)
  
  # Send request
  g <- GET(url)
  # Check if there's an error
  if (g$status_code != 200) {
    # Send error to table
    error <- as_tibble(content(g)$error$info)
    print(error)
  } else {
    # Retrieve and display the results if successful
    results <- fromJSON(content(g, "text"))$result$records
  }
} else {
  # DF to bind to
  offset <- 0
  url_string <- paste0("https://data.wprdc.org/api/3/action/datastore_search?resource_id=", resource_code, "&limit=", offset_amount)
  g <- GET(url_string)
  temp <- fromJSON(content(g, "text"))$result$records
  results <- temp
  while (nrow(temp) == offset_amount) {
    offset <- offset + offset_amount
    print(paste("Grabbing rows", offset, "through", offset + offset_amount))
    url_string <- paste0("https://data.wprdc.org/api/3/action/datastore_search?resource_id=",resource_code, "&limit=", offset_amount, "&offset=", offset)
    g <- RETRY("GET", url_string)
    temp <- fromJSON(content(g, "text"))$result$records %>%
      as_tibble()
    results <- bind_rows(results, temp)
  }
}

dbWriteTable(wh_con, SQL(paste0("Staging.", table_name)), results, overwrite= TRUE)

dbDisconnect(wh_con)