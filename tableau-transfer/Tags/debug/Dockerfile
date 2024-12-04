from countystats/python3:1.1

MAINTAINER Daniel Andrus "daniel.andrus@alleghenycounty.us"

RUN pip install --upgrade pip

RUN pip install pantab \
	tableauserverclient

COPY Tableau-Transfer_debug.py /
COPY Tableau-Transfer.py /

CMD [ "python", "-u", "Tableau-Transfer_debug.py" ]