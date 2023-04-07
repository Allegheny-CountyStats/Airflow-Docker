FROM countystats/r-basic:1.1

MAINTAINER Geoffrey Arnold "geoffrey.arnold@alleghenycounty.us"

COPY alco_geocoder.R /

COPY warehouse_geocode.R /

CMD [ "Rscript", "warehouse_geocode.R" ]