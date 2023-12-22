FROM countystats/r-basic:testing

MAINTAINER Daniel Andrus "daniel.andrus@alleghenycounty.us"

RUN install2.r --error \
    dplyr \
    janitor

COPY excel_to_staging.R /
COPY sa00427.keytab /
RUN kinit sa00427@COUNTY.ALLEGHENY.LOCAL -k -t sa00427.keytab
