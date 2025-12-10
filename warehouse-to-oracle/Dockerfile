FROM countystats/r-basic:4.3

LABEL maintainer="Geoffrey Arnold <geoffrey.arnold@alleghenycounty.us>"

COPY warehouse_to_oracle.R /

CMD [ "Rscript", "warehouse_to_oracle.R" ]