# Build from moveit-transfer/:  docker build -f tags/3.2/Dockerfile -t countystats/moveit-transfer:3.2 .
FROM countystats/r-basic:4.3

LABEL maintainer="Daniel Andrus daniel.andrus@alleghenycounty.us"

COPY moveitAPI_0.3.1.tar.gz /

RUN R -e 'install.packages("moveitAPI_0.3.0.tar.gz", type = "source")'

RUN install2.r --error \
    dplyr \
    writexl

COPY moveit-download.R /
COPY moveit-transfer.R /
