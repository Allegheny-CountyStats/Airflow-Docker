FROM countystats/r-geo:4.2

MAINTAINER Daniel Andrus "daniel.andrus@alleghenycounty.us"

# Install Kerberos
# Needs to be built within build directory/context using ktutil
COPY sa00427.keytab /

# below installs are basic, but kerberos requires the krbt-user and libpam-krb5
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

# This line creates a ticket using the keytab file within the image
RUN kinit sa00427@COUNTY.ALLEGHENY.LOCAL -k -t sa00427.keytab