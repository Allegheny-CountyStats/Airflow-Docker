require(tidyverse, quietly = T)
require(jsonlite)
require(httr)
require(DBI)

dev <- Sys.getenv('DEV', "no")

if (dev == "yes") {
  dotenv::load_dot_env("./DataDotWorld/.env")
}

if (dev == "yes") {
  query_import <- fromJSON(paste0("~/GitHub/DDW_Steward_Check/SPARQL/", Sys.getenv("SPARQL_QUERY_NAME"), ".json"))
} else {
  query_import <- fromJSON(Sys.getenv("SPARQL_QUERY"))
  print(query_import)
}

dept <- Sys.getenv("DEPT")
source <- Sys.getenv('SOURCE')
table <- Sys.getenv("TABLE")

wh_host <- Sys.getenv('WH_HOST')
wh_db <- Sys.getenv('WH_DB')
wh_user <- Sys.getenv('WH_USER')
wh_pass <- Sys.getenv('WH_PASS')

ddw_org <- Sys.getenv("DDW_ORG", "alleghenycounty")
ddw_id <- Sys.getenv("DDW_ID", "alco-metadata-reporting")
Auth_Token <- Sys.getenv('DW_AUTH_TOKEN')
replace_grep <- Sys.getenv("LOOP_VAR",NA)

# Query formatting

# query_DDW_QUERIES <- function(Auth_Token, ddw_owner, ddw_ID) {
#   url <- paste0("https://api.data.world/v0/projects/",ddw_owner,"/",ddw_ID,"/queries")
#   response <- VERB("GET", url,
#                    add_headers('Authorization' = paste('Bearer',Auth_Token)), 
#                    content_type("application/octet-stream"), 
#                    accept("application/json")
#   )
#   raise <- suppressMessages(httr::content(response, "text"))
#   result <- jsonlite::fromJSON(raise)
#   dfs <- lapply(result$records, data.frame, stringsAsFactors = FALSE)
#   FINAL_set <- do.call(cbind.data.frame, dfs)
#   new_names <- names(result$records)[-9]
#   colnames(FINAL_set) <- new_names
#   return(FINAL_set)
# }
# 
# queries_list <- query_DDW_QUERIES(Auth_Token, ddw_org, ddw_id)
# query_raw <- as.character(queries_list[queries_list$name == paste0(query_name),"body"])
query_raw <- query_import$query
if(!is.na(replace_grep)){
  query_raw<- gsub("%LOOP_LOOP%",replace_grep,query_raw)
}
print(query_raw)
query_string <- list(query = query_raw)

# DDW Query Functions
user_agent <- function() {
  ret <- sprintf("dwapi-R - %s", "X.X.X")
  ret
}

next_page <- function(next_token, Auth_Token) { #Funciton to paginate multi-page responses
  NextPage <- 
    httr::GET(
      paste0("https://api.data.world/v0/",next_token),
      httr::add_headers(
        Accept = "application/json",
        Authorization = sprintf("Bearer %s", Auth_Token)
      ),
      httr::user_agent(user_agent())
    )
  raise <- suppressMessages(httr::content(NextPage, "text"))
  result <- jsonlite::fromJSON(raise)
  return(result)
}

next_page_add <- function (api_result, Auth_Token) {
  if(any(grepl("^next",names(api_result),ignore.case = TRUE))){
    TokentDetect <- 0
    Return_Frame <- api_result$records%>%
      jsonlite::flatten()
    while(TokentDetect == 0){
      NextPage <- next_page(api_result[grep("^next", names(api_result))],Auth_Token)
      if (!any(grepl("^next",names(NextPage),ignore.case = TRUE))){
        TokentDetect <- 1
      }
      api_result <- NextPage #overrides the api_result variable to prevent infinity loop
      Return_Frame <- bind_rows(Return_Frame, NextPage$records %>% jsonlite::flatten())
    }
  }else{
    Return_Frame <- api_result
  }
  return(Return_Frame)
}

query_DDW_SPARQL <- function(Auth_Token, queryString, ddw_owner, ddw_ID) {
  url <- paste0("https://api.data.world/v0/sparql/",ddw_owner,"/",ddw_ID)
  response <- VERB("GET", url, query = queryString, 
                   add_headers('Authorization' = paste('Bearer',Auth_Token)), 
                   content_type("application/octet-stream"), 
                   accept("application/sparql-results+json, application/sparql-results+xml, application/rdf+json, application/rdf+xml, text/tab-separated-values, text/turtle, text/csv")
  )
  print("response generated")
  raise <- suppressMessages(httr::content(response, "text"))
  print("text response formatted")
  result <- jsonlite::fromJSON(raise)
  print("result generated, doing next page add")
  Collections <- next_page_add(result,Auth_Token)#check for next page in api return set
  print("exporting bindings")
  Collections <- Collections$results$bindings
  print("producing final set")
  FINAL_set <- Collections %>% 
    tidyr::unnest(result$head$vars, names_sep = "_")%>%
    dplyr::select(-contains("_type"))
  print("Returned query as Final Set")
  return(FINAL_set)
}

print(query_string$query)
QueryReturn <- query_DDW_SPARQL(Auth_Token, queryString = query_string, ddw_owner = ddw_org, ddw_ID = ddw_id)
print("Query complete")

# Connect to DataWarehouse
wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", 
                    server = wh_host,
                    database = wh_db, 
                    UID = wh_user, 
                    pwd = wh_pass)

# Write Table
table_name <- paste(dept, source, table, sep = "_")

if(!is.na(replace_grep)){
  new_table <- paste0("Staging.", paste(dept, source, table, sep = "_"))
  prel_table <- paste0("Staging.", table_name)
  if (dbExistsTable(wh_con, SQL(new_table))) {
    cols <- paste0("SELECT COLUMN_NAME
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_NAME = '", table_name, "' AND TABLE_SCHEMA = 'Staging'")
    col_names <- dbGetQuery(wh_con, cols)$COLUMN_NAME %>%
      paste(collapse = "], [")
    
    # Append to Master Table
    sql_insert <- paste0("WITH NewData AS (SELECT * FROM ", prel_table, ")
                        INSERT INTO ", new_table, " ([", col_names, "]) SELECT * FROM NewData")
    y <- dbExecute(wh_con, sql_insert)
    print(paste(y, "records added to", new_table))
  }else{
    print("writing table")
    dbWriteTable(wh_con, SQL(prel_table), QueryReturn)
  }
}else{
  print("writing table overwrite NO LOOP REPLACE DETECTED")
  dbWriteTable(wh_con, SQL(paste("Staging", table_name, sep =".")), QueryReturn, overwrite = TRUE)
}

