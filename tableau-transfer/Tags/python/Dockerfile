from countystats/python3:10

MAINTAINER Geoffrey Arnold "geoffrey.arnold@alleghenycounty.us"

RUN pip install --upgrade pip

RUN pip install SQLAlchemy==2.0.20
RUN pip install pandas==2.0.3
RUN pip install pantab==3.0.0
RUN pip install tableauhyperapi==0.0.17537
RUN pip install numpy==1.25.2

COPY Allegheny-ENTCRT01-CA.crt /usr/local/share/ca-certificates/
RUN chmod 644 /usr/local/share/ca-certificates/Allegheny-ENTCRT01-CA.crt && update-ca-certificates

ENV REQUESTS_CA_BUNDLE='/etc/ssl/certs/ca-certificates.crt'
ENV SSL_CERT_FILE='/etc/ssl/certs/ca-certificates.crt'

COPY Tableau-Transfer.py /

CMD [ "python", "-u", "Tableau-Transfer.py" ]