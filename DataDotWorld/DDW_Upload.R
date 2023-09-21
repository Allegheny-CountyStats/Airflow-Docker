require(tidyverse, quietly = T)
require(jsonlite)
require(httr)
require(DBI)
require(writexl)

dev <- Sys.getenv('DEV', "no")# Change to yes when testing locally

if (dev == "yes") {
  dotenv::load_dot_env("./.env")
}

dept <- Sys.getenv("DEPT")
source <- Sys.getenv('SOURCE')
table <- Sys.getenv("TABLE")
schema <- Sys.getenv("SCHEMA", "Master")

wh_host <- Sys.getenv('WH_HOST')
wh_db <- Sys.getenv('WH_DB')
wh_user <- Sys.getenv('WH_USER')
wh_pass <- Sys.getenv('WH_PASS')

ddw_org <- Sys.getenv("DDW_ORG", "alleghenycounty")
ddw_id <- Sys.getenv("DDW_ID", "alco-metadata-reporting")
Auth_Token <- Sys.getenv('DW_AUTH_TOKEN')
Project_Filename <- Sys.getenv('PROJECT_NAME')

# Connect to DataWarehouse
wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", 
                    server = wh_host,
                    database = wh_db, 
                    UID = wh_user, 
                    pwd = wh_pass)

# Load Table
table_name <- paste(dept, source, table, sep = "_")
New_Data <- dbReadTable(wh_con,SQL(paste(schema,table_name,sep = ".")))
write_xlsx(New_Data, Project_Filename, format_headers = TRUE)

# Upload to DDW Project
upload_url <- URLencode(paste0("https://api.data.world/v0/uploads/",ddw_org,"/",ddw_id,"/files"))
r <- VERB("POST",upload_url, body = list(file = upload_file(Project_Filename)),
          add_headers('Authorization' = paste('Bearer',Auth_Token)),
          content_type("multipart/form-data"),
          accept("application/json"))
