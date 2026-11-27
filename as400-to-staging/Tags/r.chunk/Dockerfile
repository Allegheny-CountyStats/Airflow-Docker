FROM countystats/r-basic:1.2

LABEL maintainer="Daniel Andrus daniel.andrus@alleghenycounty.us"

COPY as400_to_staging_chunk.R /

CMD [ "Rscript", "as400_to_staging_chunk.R" ]