from countystats/r-basic:4.2

MAINTAINER Daniel Andrus "daniel.andrus@alleghenycounty.us"

RUN install2.r --error \
   	dplyr \
	DBI \
	writexl

RUN fc-cache -fv

RUN apt-get update --allow-releaseinfo-change && apt-get install -y \
    python3-pip

RUN pip3 install pandas \
    oauthlib \
    requests_oauthlib \
    SQLAlchemy \
    pantab \
    pyodbc

ADD DDW_SPARQL_Query.R /
ADD DDW_Upload.R /
ADD Send_DDW_Email.py /
ADD send_email.py /
ADD Unfilled_Metadata_Email /Unfilled_Metadata_Email