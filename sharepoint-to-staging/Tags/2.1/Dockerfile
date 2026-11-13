FROM countystats/python3:10.2

LABEL maintainer="Daniel Andrus daniel.andrus@alleghenycounty.us"

RUN pip install oauthlib requests-oauthlib xlrd openpyxl

COPY sharepoint_to_staging.py /
COPY sharepoint_upload.py /

RUN apt-get --allow-releaseinfo-change update
RUN apt-get update                             \
     && apt-get install -y --no-install-recommends \
        ca-certificates curl python3-certifi

ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt


CMD [ "python", "-u", "sharepoint_to_staging.py" ]