from countystats/r-basic:4.3

MAINTAINER Geoffrey Arnold "geoffrey.arnold@alleghenycounty.us"

RUN install2.r --error \
    dplyr

ADD inter_warehouse_transfer.R /

CMD [ "Rscript", "inter_warehouse_transfer.R" ]