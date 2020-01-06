FROM python:3.6

MAINTAINER Geoffrey Arnold "geoffrey.arnold@alleghenycounty.us"

RUN apt-get update && apt-get install -y \
	unixodbc-dev \
	libaio-dev \
	sudo

COPY instantclient_19_5 ./

RUN export ORACLE_HOME=$(pwd)/instantclient_19_5

RUN export ORACLE_HOME=/location/of/your/files/instantclient_19_5
RUN export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ORACLE_HOME

RUN sudo ldconfig

ADD odbcinst.ini /etc/

COPY requirements.txt ./

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt