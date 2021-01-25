FROM python:3.7

RUN apt-get update && apt-get install -y \
    python3 python3-pip \
    fonts-liberation libappindicator3-1 libasound2 libatk-bridge2.0-0 \
    libnspr4 libnss3 lsb-release xdg-utils libxss1 libdbus-glib-1-2 \
    curl unzip wget \
    xvfb

# install geckodriver and firefox

RUN GECKODRIVER_VERSION=`curl https://github.com/mozilla/geckodriver/releases/latest | grep -Po 'v[0-9]+.[0-9]+.[0-9]+'` && \
    wget https://github.com/mozilla/geckodriver/releases/download/$GECKODRIVER_VERSION/geckodriver-$GECKODRIVER_VERSION-linux64.tar.gz && \
    tar -zxf geckodriver-$GECKODRIVER_VERSION-linux64.tar.gz -C /usr/local/bin && \
    chmod +x /usr/local/bin/geckodriver && \
    rm geckodriver-$GECKODRIVER_VERSION-linux64.tar.gz

RUN FIREFOX_SETUP=firefox-setup.tar.bz2 && \
    apt-get purge firefox && \
    wget -O $FIREFOX_SETUP "https://download.mozilla.org/?product=firefox-latest&os=linux64" && \
    tar xjf $FIREFOX_SETUP -C /opt/ && \
    ln -s /opt/firefox/firefox /usr/bin/firefox && \
    rm $FIREFOX_SETUP

ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y \
	libaio-dev \
	sudo

COPY instantclient_19_5 ./

RUN export ORACLE_HOME=$(pwd)/instantclient_19_5

RUN export ORACLE_HOME=/location/of/your/files/instantclient_19_5
RUN export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ORACLE_HOME

RUN sudo ldconfig

RUN apt-get update \
    && apt-get -y install \
        apt-transport-https \
        curl \
	unixodbc \
        unixodbc-dev \
    && apt-get clean

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get -y install \
        msodbcsql17 \
        libgssapi-krb5-2 \
    && apt-get clean  

COPY requirements.txt /

RUN pip3 install --upgrade pip && \
    pip3 install --no-cache-dir -r requirements.txt