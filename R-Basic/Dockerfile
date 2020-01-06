FROM rocker/tidyverse

MAINTAINER Geoffrey Arnold "geoffrey.arnold@alleghenycounty.us"

# system libraries of general use
RUN apt-get update && apt-get install -y \
    sudo \
    pandoc \
    pandoc-citeproc \
    libcurl4-gnutls-dev \
    libcairo2-dev \
    libxt-dev \
    libssl-dev \
    libssh2-1-dev \
    curl \
    libmpfr-dev \
    unixodbc \
    unixodbc-dev \
    freetds-dev \
    freetds-bin \
    tdsodbc \
    build-essential \
    ca-certificates \
    gcc \
    git \
    libpq-dev \
    make \
    python-pip \
    python2.7 \
    python2.7-dev \
    ssh \
    putty \
    software-properties-common \
    && apt-get autoremove \
    && apt-get clean

# Rewrite odbcinst.ini file for connectors
COPY etc_odbcinst.ini /etc/odbcinst.ini

# Add Repo and install JDK 8
RUN apt-get update -qq && apt-get -y --no-install-recommends install \
  libxml2-dev \
  libcairo2-dev \
  libpq-dev \
    libicu-dev \ 
    libbz2-dev \
  liblzma-dev \
  default-jdk \
  && R CMD javareconf

# install dependencies of most cron jobs
RUN install2.r --error \
    rJava \
    odbc \
    DBI \
    RODBC \
    RJDBC \
    httr \
    jsonlite \
    RCurl \
    readxl \
    zoo \
    rlang \
    XML \
    httr \
    dotenv \
    snakecase

# Copy of java connector drivers
# Oracle
COPY ojdbc6.jar /lib/ojdbc6.jar
# AS 400
COPY jt400-jdk9-10.1.jar /lib/jt400-jdk9-10.1.jar