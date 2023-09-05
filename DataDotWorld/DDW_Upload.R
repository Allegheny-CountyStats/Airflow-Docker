require(tidyverse, quietly = T)
require(jsonlite)
require(httr)
require(DBI)

dev <- Sys.getenv('DEV', "no")# Change to yes when testing locally

if (dev == "yes") {
  dotenv::load_dot_env("./.env")
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
Upload_Type <- Sys.getenv("PATCH_or_POST","PATCH")

# Load Data from SQL Server
