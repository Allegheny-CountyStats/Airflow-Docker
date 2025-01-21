from countystats/r-basic:4.2

RUN install2.r --error \
   	dplyr \
	DBI

MAINTAINER Geoffrey Arnold "geoffrey.arnold@alleghenycounty.us"

COPY staging_to_warehouse.R /

COPY staging_append_warehouse.R /

COPY staging_replace_warehouse.R /