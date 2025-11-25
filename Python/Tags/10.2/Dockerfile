FROM python:3.10-bookworm

LABEL maintainer="Daniel Andrus daniel.andrus@alleghenycounty.us"

RUN apt-get update && apt-get install -y \
	libaio-dev \
	sudo

COPY instantclient_19_5 ./

COPY *.crt /usr/local/share/ca-certificates/

RUN apt-get --allow-releaseinfo-change update
RUN apt-get update                             \
    && apt-get install -y --no-install-recommends \
        ca-certificates wget 

RUN export ORACLE_HOME=$(pwd)/instantclient_19_5

RUN export ORACLE_HOME=/location/of/your/files/instantclient_19_5
RUN export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ORACLE_HOME

RUN sudo ldconfig

RUN apt-get update \
    && apt-get -y install \
        apt-transport-https \
        curl \
    && apt-get clean

COPY Allegheny-ENTCRT01-CA.crt /usr/local/share/ca-certificates/
COPY ACUS2026.crt /usr/local/share/ca-certificates/
RUN chmod 644 /usr/local/share/ca-certificates/Allegheny-ENTCRT01-CA.crt  \
    && chmod 644 /usr/local/share/ca-certificates/ACUS2026.crt \
    && update-ca-certificates

RUN export DEBIAN_FRONTEND=noninteractive

RUN curl -O https://packages.microsoft.com/config/debian/12/packages-microsoft-prod.deb \
    && dpkg -i packages-microsoft-prod.deb \
    && rm packages-microsoft-prod.deb \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get -y install \
        msodbcsql17 \
        mssql-tools \
        msodbcsql18 \
        mssql-tools18 \
        unixodbc-dev \
        libgssapi-krb5-2 \
    && apt-get clean

COPY requirements.txt ./

RUN pip3 install --upgrade pip && \
    pip3 install --no-cache-dir -r requirements.txt

ENV REQUESTS_CA_BUNDLE='/etc/ssl/certs/ca-certificates.crt'
ENV SSL_CERT_FILE='/etc/ssl/certs/ca-certificates.crt'
