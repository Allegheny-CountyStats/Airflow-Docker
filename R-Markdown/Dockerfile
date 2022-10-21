FROM countystats/r-geo:4.2

MAINTAINER Geoffrey Arnold "geoffrey.arnold@alleghenycounty.us"

RUN install2.r --error \
    ggrepel
    
RUN apt-get --allow-releaseinfo-change update && apt-get install -y \
    texlive \
    texlive-full \
    imagemagick
    
RUN fc-cache -fv

COPY send_email.py \