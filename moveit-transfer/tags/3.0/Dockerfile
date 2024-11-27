from countystats/r-basic:4.3

MAINTAINER Geoffrey Arnold "geoffrey.arnold@alleghenycounty.us"

COPY moveitAPI_0.3.0.tar.gz /

RUN R -e 'install.packages("moveitAPI_0.3.0.tar.gz", type = "source")'

RUN install2.r --error \
    dplyr

COPY moveit-download.R /
COPY moveit-transfer.R /