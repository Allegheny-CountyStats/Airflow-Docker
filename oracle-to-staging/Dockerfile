FROM countystats/r-basic:latest

MAINTAINER Geoffrey Arnold "geoffrey.arnold@alleghenycounty.us"

RUN install2.r --error \
    dplyr

COPY oracle_to_staging.R /

CMD [ "Rscript", "oracle_to_staging.R" ]