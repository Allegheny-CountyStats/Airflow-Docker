FROM countystats/r-basic:4.2

RUN install2.r --error \
   	dplyr \
	DBI

LABEL maintainer="Daniel Andrus daniel.andrus@alleghenycounty.us"

COPY staging_to_warehouse.R /

COPY staging_append_warehouse.R /

COPY staging_replace_warehouse.R /