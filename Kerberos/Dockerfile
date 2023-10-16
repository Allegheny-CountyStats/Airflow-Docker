FROM countystats/r-geo:4.2

MAINTAINER Daniel Andrus "daniel.andrus@alleghenycounty.us"

# Install Kerberos
# Needs to be buil within build directory/context using ktutil
COPY sa00427.keytab /

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

RUN kinit sa00427@COUNTY.ALLEGHENY.LOCAL -k -t sa00427.keytab