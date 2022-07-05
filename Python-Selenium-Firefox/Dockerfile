FROM countystats/python3:10

RUN mkdir ~/downloads

RUN apt-get update                             \
     && apt-get install -y --no-install-recommends \
        ca-certificates curl firefox-esr           \
     && rm -fr /var/lib/apt/lists/*                

COPY geckodriver /usr/bin/geckodriver
RUN sudo chmod +x /usr/bin/geckodriver

RUN pip3 install --upgrade pip && \
    pip3 install --no-cache-dir selenium