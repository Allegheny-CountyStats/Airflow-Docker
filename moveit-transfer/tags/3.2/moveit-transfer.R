require(moveitAPI)
require(dplyr)
require(DBI)
require(snakecase)

options(scipen=999)

# dotenv::load_dot_env()

table <- Sys.getenv('TABLE')
schema <- Sys.getenv('SCHEMA', 'Reporting') 
  
user <- Sys.getenv('MI_USER')
pass <- Sys.getenv('MI_PASS')

wh_host <- Sys.getenv('WH_HOST')
wh_db <- Sys.getenv('WH_DB')
sa_user <- Sys.getenv('SA_USER')
sa_pass <- Sys.getenv('SA_PASS')
wh_user <- Sys.getenv('WH_USER', sa_user)
wh_pass <- Sys.getenv('WH_PASS', sa_pass)

sql <- Sys.getenv('SQL')

# csv (default) or xlsx
file_type <- tolower(Sys.getenv("FILE_TYPE", unset = "csv"))
if (!file_type %in% c("csv", "xlsx")) {
  stop("FILE_TYPE must be csv or xlsx")
}

filename <- Sys.getenv("FILENAME", unset = paste0(tolower(table), ".", file_type))
folder <- Sys.getenv("FOLDER")
folder_path <- paste0(Sys.getenv("FOLDER_PATH"), folder)

rownames <- Sys.getenv("ROWNAMES", unset = FALSE)
rownames <- toupper(rownames) == "TRUE"

blank_na <- Sys.getenv("BLANK_NA", unset = "FALSE")

snake_case <- Sys.getenv("SNAKE_CASE", unset = "FALSE")

moveit_url <- Sys.getenv("BASE_URL", unset = "alleghenycounty.us")

# Auth MoveIt API
auth <- paste0("grant_type=password&username=", user, "&password=", pass)
tokens <- authMoveIt(moveit_url, auth)

# Look for Folder
folders <- availableFolders(moveit_url, tokens) %>%
  filter(name == folder)

if (nrow(folders) == 1) {
    id <- folders$id[[1]]
} else if (nrow(folders) > 1 & folder_path != "") {
  folders <- availableFolders(moveit_url, tokens) %>%
    filter(path == folder_path)
  id <- folders$id[[1]]
} else if (nrow(folders) > 1 & folder_path == "") {
  stop("Multiple instances of this folder name! Provide Folder Path")
} else {
  stop("Provided folder name does not exist!")
}

print(id)

# Connect to DataWarehouse
wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = wh_db, UID = wh_user, pwd = wh_pass)

# Query Table
if (sql == "") {
  temp <- dbReadTable(wh_con, SQL(paste0(schema, "." , table)))
} else {
  temp <- dbGetQuery(wh_con, sql)
}

# Warehouse Disconnect
dbDisconnect(wh_con)

# Transform Colnames to snake_case
if (snake_case == "TRUE") {
  colnames(temp) <- to_snake_case(colnames(temp))
}

# Write file for upload
if (file_type == "xlsx") {
  # writexl always writes NA as a blank cell and has no row names
  writexl::write_xlsx(temp, filename)
  content_type <- "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
} else {
  if (blank_na == "TRUE") {
    write.csv(temp, filename, row.names = rownames, na = "")
  } else {
    write.csv(temp, filename, row.names = rownames)
  }
  content_type <- "text/csv"
}

# Upload to MoveIt
uploadMoveItFile(moveit_url, tokens, id, filename, content_type)
