FROM countystats/r-geo:1.2

MAINTAINER Geoffrey Arnold "geoffrey.arnold@alleghenycounty.us"

COPY us_counties.geojson /

COPY usps_geocoder.R /

COPY usps-geocode.R /

CMD [ "Rscript", "usps-geocode.R" ]