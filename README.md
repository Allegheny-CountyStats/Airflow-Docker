# Airflow-Docker
Safe, public Docker containers, images, and common tasks for data ETL for CountyStats Airflow needs

# Instructions
By and large the Docker Operator portions you will setting are, task_id, image,command and the relevant environnment dictionary portions.
Note that many of the default environmental variables are only applicable for internal use at CountyStat. If you are running into issues using these images in your own enviornment these are the likely source of those issues.

# Containers:
- [Python-Selenium](https://hub.docker.com/repository/docker/countystats/selenium): Python and Selenium image used to use the Chrome browser for web-scraping in `python` scripts. Based on an image created by [joyzoursky](https://github.com/joyzoursky/docker-python-chromedriver).
- Python-Selenium-Firefox: Abandoned attempt to get Selenium to work on Linux with Firefox browser instead of Chrome.
- [Python](https://hub.docker.com/repository/docker/countystats/r-geo): Python 3 Docker container with common python modules installed.
- [R-Basic](https://hub.docker.com/repository/docker/countystats/r-basic): R Docker container with common R packages installed.
- [R-Geo](https://hub.docker.com/repository/docker/countystats/r-geo): R Docker container with common geospatial packages installed.

# Images:

# Other:
- [migration] Simple R script for transferring a table from Development to Production or vice-versa.
