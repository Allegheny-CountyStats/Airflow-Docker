# Airflow-Docker
Safe, public Docker containers, images, and common tasks for data ETL for CountyStats Airflow needs

# Instructions
By and large the Docker Operator portions you will setting are, task_id, image,command and the relevant environnment dictionary portions.
Note that many of the default environmental variables are only applicable for internal use at CountyStat. If you are running into issues using these images in your own enviornment these are the likely source of those issues.

# Containers:

- Python-Selenium-Firefox: Abandoned attempt to get Selenium to work on Linux with Firefox browser instead of Chrome.
- [Python](https://hub.docker.com/repository/docker/countystats/r-geo): Python 3 Docker container with common python modules installed.
- [R-Basic](https://hub.docker.com/repository/docker/countystats/r-basic): R Docker container with common R packages installed.
- [R-Geo](https://hub.docker.com/repository/docker/countystats/r-geo): R Docker container with common geospatial packages installed.

# Images:
- [Python-Selenium](https://hub.docker.com/repository/docker/countystats/selenium): Python and Selenium image used to use the Chrome browser for web-scraping in `python` scripts. Based on an image created by [joyzoursky](https://github.com/joyzoursky/docker-python-chromedriver).
*	as400-to-staging: Trasnfer Data to the Data Warehouse for schema verification.
*	cj_to_warehouse: Transfer data from the Criminal Justice Data Warehouse to the normal DataWarehouse.
*	data-validate: Data Schema Validation to ensure column values contain expected value types
*	datetime-transform: Sometimes Oracle datetimes are not properly formatted when brought into the Data Warehouse. If there are too many to write out the SQL, this task can handle them for you.
*	geocoder: Use the ALCO Geocoder script to geocode addresses in the DataWarehouse.
*	moveit-transfer: Transfer Data to and from the DataWarehouse and the MoveIt Server
*	moveit-transfer-geo: Transfer Geographic data to and from the Data Warehouse and the MoveIt Server.
*	mssql-to-staging: Transfer Data from MSSQL to the Data Warehouse for schema verification
*	oracle-to-staging: Transfer Data from Oracle to the Data Warehouse for schema verfication
*	sharepoint-to-staging: Transfer Data from Sharepoint/Office365/OneDrive to the Data Warehouse for schema verification.
*	staging-to-warehouse: Transfer data from the Staging schema to its Department schema after data validation.
*	tablea-transfer: Transfer data from the Datawarehouse to the Tableau Server
*	usps-geocoder: Use the USPS Geocoder script to geocode addresses in the DataWarehouse.
*	warehouse-to-oracle: Write a CountyStat Data Warehouse Table to the DHS Data Warehouse
*	wprdc-to-staging: Transfer Data from the WPRDC to the Datawarehouse for schema verification.

# Other:
* migration: Simple R script for transferring a table from Development to Production or vice-versa.
