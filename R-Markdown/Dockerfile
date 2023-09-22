FROM countystats/r-geo:4.2

MAINTAINER Daniel Andrus "daniel.andrus@alleghenycounty.us"

RUN install2.r --error \
    ggrepel
    
RUN apt-get --allow-releaseinfo-change update && apt-get install -y \
    texlive \
    texlive-full \
    imagemagick
    
RUN fc-cache -fv