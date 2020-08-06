FROM rocker/tidyverse:3.6.3

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
    RJDBC \
    httr \
    jsonlite \
    RCurl \
    readxl \
    zoo \
    rlang \
    httr \
    dotenv \
    snakecase \
    plotly \
    pander \
    kableExtra \
    treemap

# Copy of java connector drivers
# Oracle
COPY ojdbc6.jar /lib/ojdbc6.jar
# AS 400
COPY jt400-jdk9-10.1.jar /lib/jt400-jdk9-10.1.jar

RUN apt-get update \
    && apt-get -y install \
        apt-transport-https \
        curl \
    && apt-get clean

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get -y install \
        msodbcsql17 \
        libgssapi-krb5-2 \
    && apt-get clean
    
RUN  R -e "devtools::install_github('Allegheny-CountyStats/moveitAPI')" 