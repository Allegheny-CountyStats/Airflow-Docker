FROM countystats/r-geo:4.2

MAINTAINER Daniel Andrus "daniel.andrus@alleghenycounty.us"

RUN install2.r --error \
    ggthemes \
    circlize

RUN apt-get update --allow-releaseinfo-change && apt-get install -y \
    texlive \
    texlive-full \
    imagemagick

RUN fc-cache -fv

RUN apt-get update --allow-releaseinfo-change && apt-get install -y \
    python3-pip

RUN pip3 install pandas \
    oauthlib \
    requests_oauthlib

RUN install2.r --error \
    ggrepel \
    gghighlight \
    DescTools \
    treemapify \
    bizdays \
    formattable \
    calendR \
    pracma \
    patchwork \
    ggplot2 \
    janitor
