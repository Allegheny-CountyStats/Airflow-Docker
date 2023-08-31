library(httr)

dev <- Sys.getenv('DEV', "no")

if (dev == "yes") {
  dotenv::load_dot_env("./DataDotWorld/.env")
}

ddw_org <- Sys.getenv("DDW_ORG", "alleghenycounty")
Auth_Token <- Sys.getenv('DW_AUTH_TOKEN')

url <- "https://api.data.world/v0/metadata/collections/alleghenycounty"

response <- VERB("GET", url, 
                 add_headers('Authorization' = paste('Bearer',Auth_Token)), 
                 content_type("application/octet-stream"), accept("application/json"))

raise <- content(response, "text")
result <- jsonlite::fromJSON(raise)
Collections <- next_page_add(result,Auth_Token)
print(Collections$title)
