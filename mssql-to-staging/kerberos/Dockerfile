FROM countystats/r-basic:4.2.1

LABEL org.opencontainers.image.authors="Geoffrey.Arnold@alleghenycounty.us"

RUN apt-get update && \
    DEBIAN_FRONTEND='noninteractive' apt-get install -y --no-install-recommends \
    ca-certificates\
    realmd\
    libpam-sss\
    libnss-sss\
    sssd\
    sssd-tools\
    adcli\
    krb5-user\
    libpam-krb5

ADD mssql_to_staging.R /

CMD ["Rscript", "mssql_to_staging.R"]