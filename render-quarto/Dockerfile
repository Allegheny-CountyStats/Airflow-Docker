FROM countystats/r-geo:4.2

LABEL org.opencontainers.image.authors="geoffrey.arnold@alleghenycounty.us"

RUN apt-get update && apt-get install -y --no-install-recommends \
    pandoc \
    pandoc-citeproc \
    curl \
    gdebi-core \
    && rm -rf /var/lib/apt/lists/*

COPY quarto-1.6.40-linux-amd64.deb /

RUN dpkg -i quarto-1.6.40-linux-amd64.deb

RUN apt-get update && apt-get install -y \
    python3-pip

RUN python3 -m pip install jupyter

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
    janitor \
    cowplot \
    ggforce \
    quarto \
    markdown \
    tidyverse \
    hms \
    xfun \
    knitr \
    kableExtra \
    palmerpenguins \
    leaflegend \
    ggtext

COPY CountyStatKPI_1.0.11.tar.gz /

RUN Rscript -e "install.packages('CountyStatKPI_1.0.11.tar.gz', repos = NULL, type = 'source')"

RUN Rscript -e "devtools::install_version('plotly', version = '4.10.4')"

COPY _extensions/ _extensions/