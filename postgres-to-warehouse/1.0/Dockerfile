FROM countystats/r-basic:4.2

RUN install2.r --error \
   	DBI \
	  RPostgres

LABEL maintainer="Daniel Andrus daniel.andrus@alleghenycounty.us"

COPY postgres_to_warehouse.R /
CMD ["Rscript", "postgres_to_warehouse.R"]