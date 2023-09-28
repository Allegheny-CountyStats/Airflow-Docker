FROM countystats/r-geo:4.2

MAINTAINER Daniel Andrus "daniel.andrus@alleghenycounty.us"

# Install Kerberos
RUN apt-get update && \
    DEBIAN_FRONTEND='noninteractive' apt-get install -y --no-install-recommends \
    realmd\
    libpam-sss\
    libnss-sss\
    sssd\
    sssd-tools\
    adcli\
    krb5-user\
    libpam-krb5

COPY krb5.conf /etc/
# COPY krb5.keytab /etc/ # THIS DOESN'T WORK 

# ++++++++++++++++_____________debugging step, remove upon final build
ENTRYPOINT ["tail", "-f", "/dev/null"]
# ++++++++++++++++_____________debugging step, remove upon final build