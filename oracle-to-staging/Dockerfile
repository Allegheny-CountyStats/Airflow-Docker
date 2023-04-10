from countystats/r-basic:1.1

MAINTAINER Geoffrey Arnold "geoffrey.arnold@alleghenycounty.us"

RUN install2.r --error \
    dplyr

COPY oracle_to_staging.R /

CMD [ "Rscript", "oracle_to_staging.R" ]