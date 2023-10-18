from countystats/r-geo:4.2

MAINTAINER Geoffrey Arnold "geoffrey.arnold@alleghenycounty.us"

RUN install2.r --error \
    dplyr

ADD gis_load.R /

CMD [ "Rscript", "gis_load.R" ]