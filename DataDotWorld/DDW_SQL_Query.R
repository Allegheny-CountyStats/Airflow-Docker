require(tidyverse, quietly = T)
require(jsonlite)
require(httr)
require(DBI)
require(readr)

dev <- Sys.getenv('DEV', "no")# Change to yes when testing locally

if (dev == "yes") {
  dotenv::load_dot_env("./.env")
  dev <- "yes"
}

query_id <- Sys.getenv("SQL_QUERY_ID", NA)
sql_query <- Sys.getenv("SQL_QUERY", NA)
dept <- Sys.getenv("DEPT")
source <- Sys.getenv('SOURCE')
table <- Sys.getenv("TABLE")

wh_host <- Sys.getenv('WH_HOST')
wh_db <- Sys.getenv('WH_DB')
wh_user <- Sys.getenv('WH_USER')
wh_pass <- Sys.getenv('WH_PASS')

accept_null_return <- Sys.getenv("ACCEPT_NULL_RETURN", "yes") # Specifies if null returns from query should be accepted
ddw_org <- Sys.getenv("DDW_ORG", "alleghenycounty")
ddw_id <- Sys.getenv("DDW_ID", "alco-metadata-reporting")
Auth_Token <- Sys.getenv('DW_AUTH_TOKEN')
replace_grep <- Sys.getenv("LOOP_VAR",NA)# If task used in loop, this specifies variable to employ in place of %LOOP_LOOP% placeholder  

# DDW Query Functions
user_agent <- function() {
  ret <- sprintf("dwapi-R - %s", "X.X.X")
  ret
}

get_query <- function(Auth_Token, query_id) {
  url <- paste0("https://api.data.world/v0/queries/",query_id)
  
  response <- VERB("GET", url, add_headers('Authorization' = paste('Bearer',Auth_Token)), 
                   content_type("application/octet-stream"), accept("application/json"))
  raise <- suppressMessages(httr::content(response, "text"))
  result <- jsonlite::fromJSON(raise)
  return(result$body)
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
      Return_Frame <- Return_Frame$results$bindings
    }
  }else{
    Return_Frame <- api_result
  }
  return(Return_Frame)
}

query_DDW_SQL <- function(Auth_Token, queryString, ddw_owner, ddw_ID, AcceptNullReturn) {
  url <- paste0("https://api.data.world/v0/sql/",ddw_owner,"/",ddw_ID)
  response <- VERB("GET", url, query = queryString, 
                   add_headers('Authorization' = paste('Bearer',Auth_Token)), 
                   content_type("application/octet-stream"), 
                   accept("application/json, application/json-l, application/x-ndjson, text/csv")
  )
  print("response generated")
  raise <- suppressMessages(httr::content(response, "text"))
  print("text response formatted")
  result <- jsonlite::fromJSON(raise)
  print("result generated, doing next page add")
  Collections <- next_page_add(result,Auth_Token)#check for next page in api return set
  if (is.null(nrow(Collections)) & AcceptNullReturn == "yes"){
  }else if (is.null(nrow(Collections)) & AcceptNullReturn == "no"){
      stop("No rows returned (no results$bindings) and ACCEPT_NULL_RETURN is set to 'no'")
  }else{
    print("producing final set")
    FINAL_set <- Collections %>% 
      tidyr::unnest(result$head$vars, names_sep = "_")%>%
      dplyr::select(-contains("_type"))
    print("Returned query as Final Set")
    return(FINAL_set)
    }
}

# Import query from env variable
if(!is.na(query_id)){
  query_raw <- get_query(Auth_Token, query_id)
}else if (!is.na(sql_query)){
  query_raw <- sql_query
}

if(!is.na(replace_grep)){
  query_raw<- gsub("%LOOP_LOOP%",replace_grep,query_raw)
}
print(query_raw)
query_string <- list(query = query_raw)

# Run Query
print(query_string$query)
QueryReturn <- query_DDW_SQL(Auth_Token, queryString = query_string, ddw_owner = ddw_org, ddw_ID = ddw_id, accept_null_return)
print("Query complete")

# Connect to DataWarehouse
wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", 
                    server = wh_host,
                    database = wh_db, 
                    UID = wh_user, 
                    pwd = wh_pass)

# Write Table
table_name <- paste(dept, source, table, sep = "_")
if (!is.null(QueryReturn)){
  if(!is.na(replace_grep)){
    new_table <- paste0("Staging.", paste(dept, source, table, sep = "_"))
    if (dbExistsTable(wh_con, SQL(new_table))) {
      prel_table <- paste0("Staging.NEW_", table_name)
      dbWriteTable(wh_con, SQL(prel_table), QueryReturn, overwrite = TRUE)
      cols <- paste0("SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '", table_name, "' AND TABLE_SCHEMA = 'Staging'")
      col_names <- dbGetQuery(wh_con, cols)$COLUMN_NAME %>%
        paste(collapse = "], [")
      
      # Append to Staging Table
      sql_insert <- paste0("WITH NewData AS (SELECT * FROM ", prel_table, ")
                          INSERT INTO ", new_table, " ([", col_names, "]) SELECT * FROM NewData")
      y <- dbExecute(wh_con, sql_insert)
      print(paste(y, "records added to", new_table))
      # Drop Staging.NEW_ Table
      sql_drop <- paste('DROP TABLE IF EXISTS', prel_table)
      dbExecute(wh_con, sql_drop)
    }else{
      print("writing table")
      dbWriteTable(wh_con, SQL(new_table), QueryReturn)
    }
  }else{
    print("writing table overwrite NO LOOP REPLACE DETECTED")
    dbWriteTable(wh_con, SQL(paste("Staging", table_name, sep =".")), QueryReturn, overwrite = TRUE)
  }
}else if (is.null(QueryReturn) & accept_null_return == "yes"){
  if (!is.na(replace_grep)){
    print(paste("No records to add from", replace_grep))
  }else{
    print("No records to add and ACCEPT_NULL_RETURN is set to `yes`")  
    }
}else if (is.null(QueryReturn) & accept_null_return == "no"){
  stop("Query Return is Null and ACCEPT_NULL_RETURN is `no`: Error with Query as well since line 97 should prevent this message")
}

