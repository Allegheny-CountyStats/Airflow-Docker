FROM countystats/r-basic:4.3

LABEL maintainer="Geoffrey Arnold <geoffrey.arnold@alleghenycounty.us>"

RUN install2.r --error \
    RJDBC \
    DBI

COPY warehouse_to_oracle.R /

CMD [ "Rscript", "warehouse_to_oracle.R" ]