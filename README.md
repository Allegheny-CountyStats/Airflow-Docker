# Airflow-Docker
Generic Docker containers for CountyStats Airflow needs

## Base Containers 
- [Python-Selenium](https://hub.docker.com/repository/docker/countystats/selenium): Python and Selenium image used to use the Chrome browser for web-scraping in `python` scripts. Based on an image created by [joyzoursky](https://github.com/joyzoursky/docker-python-chromedriver).
- Python-Selenium-Firefox: Abandoned attempt to get Selenium to work on Linux with Firefox browser instead of Chrome.
- [Python](https://hub.docker.com/repository/docker/countystats/r-geo): Python 3 Docker container with common python modules installed.
- [R-Basic](https://hub.docker.com/repository/docker/countystats/r-basic): R Docker container with common R packages installed.
- [R-Geo](https://hub.docker.com/repository/docker/countystats/r-geo): R Docker container with common geospatial packages installed.

## Template Containers
- as400-to-staging: Trasnfer Data to the Data Warehouse for schema verification.
- cj_to_warehouse: Transfer data from the Criminal Justice Data Warehouse to the normal DataWarehouse.
- data-validate: Data Schema Validation to ensure column values contain expected value types
- datetime-transform: Sometimes Oracle datetimes are not properly formatted when brought into the Data Warehouse. If there are too many to write out the SQL, this task can handle them for you.
- geocoder: Use the [ALCO Geocoder](https://github.com/Allegheny-CountyStats/ALCO-Geocoder) script to geocode addresses in the DataWarehouse.
- moveit-transfer: Transfer Data to and from the DataWarehouse and the MoveIt Server
- moveit-transfer-geo: Transfer Geographic data to and from the Data Warehouse and the MoveIt Server.
- mssql-to-staging: Transfer Data from MSSQL to the Data Warehouse for schema verification
- oracle-to-staging: Transfer Data from Oracle to the Data Warehouse for schema verfication
- sharepoint-to-staging: Transfer Data from Sharepoint/Office365/OneDrive to the Data Warehouse for schema verification.
- staging-to-warehouse: Transfer data from the Staging schema to its Department schema after data validation.

## Other
- migration: Simple R script for transferring a table from Development to Production or vice-versa.
