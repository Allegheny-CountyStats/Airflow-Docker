require(sf)
require(readxl)
require(dplyr)

counties <- st_read("gz_2010_us_050_00_500k.json")

codes <- read_excel("all-geocodes-v2018.xlsx", skip = 4)

state_names <- codes %>%
  filter(`County Code (FIPS)` == "000", `Place Code (FIPS)` == "00000", `Consolidtated City Code (FIPS)` == "00000") %>%
  rename(STATE = `State Code (FIPS)`,
         STATE_NAME = `Area Name (including legal/statistical area description)`) %>%
  select(STATE, STATE_NAME)

counties_join <- counties %>%
  left_join(state_names, by = "STATE")

st_write(counties_join, "us_counties.geojson")
