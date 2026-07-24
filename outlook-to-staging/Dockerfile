FROM countystats/python3:10

LABEL maintainer="Daniel Andrus daniel.andrus@alleghenycounty.us"

RUN pip install oauthlib requests-oauthlib xlrd openpyxl
RUN apt-get --allow-releaseinfo-change update
RUN apt-get update                             \
     && apt-get install -y --no-install-recommends \
        python3-certifi

ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

COPY email_to_staging.py /
COPY check_email.py /

CMD [ "python", "-u", "email_to_staging.py" ]