from countystats/r-basic:latest

MAINTAINER Geoffrey Arnold "geoffrey.arnold@alleghenycounty.us"

RUN install2.r --error \
    dplyr

ADD mssql_to_staging.R /
COPY openssl.cnf /usr/lib/ssl/

CMD [ "Rscript", "mssql_to_staging.R" ]