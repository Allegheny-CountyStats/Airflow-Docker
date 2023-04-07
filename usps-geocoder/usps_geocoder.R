require(httr)
require(jsonlite)
require(utils)
require(plyr)
require(tidyverse)

uspsGeo <- function(locs) {
  # Build Empty Data frace
  final <- NULL
  # Run for each address
  for (address in locs) {
    # Encode address for API call
    address_encode <- URLencode(address, reserved = TRUE, repeated = TRUE)
    # Build URL for geocode
    url <- paste0("https://gis.usps.com/arcgis/rest/services/locators/US_Street/GeocodeServer/findAddressCandidates?Single+Line+Input=", address_encode, '&outSR=4326&f=pjson')
    # Print message for user
    message(paste("Source :", url))
    # Send geet request
    r <- httr::GET(url)
    # Load Content
    c <- jsonlite::fromJSON(httr::content(r))
  
    # Check status code & number of candidates
    if (r$status_code == 200 & length(jsonlite::fromJSON(httr::content(r))$candidates) > 0) {
      # Successful add top candidate
      longitude <- c$candidates$location[1,1]
      latitude <- c$candidates$location[1,2]
    } else if (r$status_code == 200) {
      # Error if not address candidates
      message("    Failed with error (200): No address candidates")
      longitude <- NA
      latitude <- NA
    } else {
      # Print other error
      message(paste0("    Failed with error (", c$error$code, "): ", c$error$message))
      longitude <- NA
      latitude <- NA
    }
    # Build columns for bind
    df <- data.frame(longitude, latitude)
    
    # Build Dataframe for results
    if (is.null(final)) {
      final <- df
    } else {
      # Merge to results dataframe
      final <- plyr::rbind.fill(final, df)
    }
  }
  
  return(final)
}

mutate_uspsGeo <- function (data, location, ...){
  # Get data location
  locs <- data[[deparse(substitute(location))]]
  # Run geocode script
  gcdf <- uspsGeo(locs, ...)
  # Bind to x
  dplyr::bind_cols(data, gcdf)
}
