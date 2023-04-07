require(sf)
require(DBI)
require(moveitAPI)
require(snakecase)
require(dplyr)

# dotenv::load_dot_env()

table <- Sys.getenv('TABLE')

user <- Sys.getenv('MI_USER')
pass <- Sys.getenv('MI_PASS')

filename <- Sys.getenv("FILENAME", unset = paste0(tolower(table), ".geojson"))
folder <- Sys.getenv("FOLDER")

coords <- Sys.getenv("COORDS")
cols <- unlist(strsplit(coords, ","))

wh_host <- Sys.getenv('WH_HOST')
wh_db <- Sys.getenv('WH_DB')
wh_user <- Sys.getenv('WH_USER')
wh_pass <- Sys.getenv('WH_PASS')

moveit_url <- "alleghenycounty.us"

snake_case <- Sys.getenv("SNAKE_CASE", unset = "FALSE")

# Connect to DataWarehouse
wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = wh_db, UID = wh_user, pwd = wh_pass)

# Query Table
sql <- paste("SELECT * FROM Reporting." , table, 'WHERE', cols[1], "IS NOT NULL")
temp <- dbGetQuery(wh_con, sql)

dbDisconnect(wh_con)

# Transform Colnames to snake_case
if (snake_case == "TRUE") {
  colnames(temp) <- to_snake_case(colnames(temp))
}

# Transform
temp_sp <- temp %>%
  st_as_sf(coords = cols)

# Write GEOJSON
st_write(temp_sp, filename)

# Auth MoveIt API
auth <- paste0("grant_type=password&username=", user, "&password=", pass)
tokens <- authMoveIt(moveit_url, auth)

# Look for Folder
folders <- availableFolders(moveit_url, tokens) %>%
  filter(name == folder)

if (nrow(folders) == 1) {
  id <- folders$id
} else if (nrow(folders) > 1){
  stop("Multiple instances of this folder name!")
} else {
  stop("Provided folder name does not exist!")
}

# Upload to MoveIt
uploadMoveItFile(moveit_url, tokens, id, filename, "application/geo+json")
