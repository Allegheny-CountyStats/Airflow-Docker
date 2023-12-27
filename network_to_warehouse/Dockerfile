FROM countystats/r-basic:latest

MAINTAINER Daniel Andrus "daniel.andrus@alleghenycounty.us"

RUN install2.r --error \
    dplyr \
    janitor

COPY excel_to_staging.R /
COPY network_to_warehouse.sh /
# COPY sa00427.keytab /
# RUN kinit sa00427@COUNTY.ALLEGHENY.LOCAL -k -t sa00427.keytab
