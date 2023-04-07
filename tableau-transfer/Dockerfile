from countystats/python3:1.1

MAINTAINER Geoffrey Arnold "geoffrey.arnold@alleghenycounty.us"

RUN pip install pantab \
	tableauserverclient

COPY Tableau-Transfer.py /

CMD [ "python", "-u", "Tableau-Transfer.py" ]