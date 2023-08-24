from countystats/python3:10

MAINTAINER Daniel Andrus "daniel.andrus@alleghenycounty.us"

RUN pip install --upgrade pip

RUN pip install pantab==3.0.0
RUN pip install tableauhyperapi==0.0.17537

COPY Tableau-Transfer_debug.py /
COPY Tableau-Transfer.py /

CMD [ "python", "-u", "Tableau-Transfer_debug.py" ]