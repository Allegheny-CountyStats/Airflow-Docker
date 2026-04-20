FROM countystats/python3:10.2

LABEL maintainer="Daniel Andrus daniel.andrus@alleghenycounty.us"

RUN pip install --upgrade pip

RUN pip install SQLAlchemy==2.0.20
RUN pip install pandas==2.0.3
RUN pip install pantab==3.0.0
RUN pip install tableauhyperapi==0.0.17537
RUN pip install numpy==1.25.2

COPY Tableau-Transfer.py /

CMD [ "python", "-u", "Tableau-Transfer.py" ]