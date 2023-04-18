FROM countystats/python3:10

MAINTAINER Geoffrey Arnold "geoffrey.arnold@alleghenycounty.us"

RUN pip install sodapy

RUN pip install pandas==1.5.3

RUN pip install SQLAlchemy==1.4.46

COPY pull-socrata.py /

CMD [ "python", "-u", "pull-socrata.py" ]