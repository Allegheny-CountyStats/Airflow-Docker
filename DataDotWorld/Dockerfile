from countystats/r-basic:4.2

RUN install2.r --error \
   	dplyr \
	DBI \
	writexl

MAINTAINER Daniel Andrus "daniel.andrus@alleghenycounty.us"

ADD DDW_SPARQL_Query.R /
ADD DDW_Upload.R /