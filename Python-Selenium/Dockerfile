FROM joyzoursky/python-chromedriver

RUN mkdir ~/downloads

RUN apt-get update \
    && apt-get -y install \
        apt-transport-https \
        curl \ 
        sudo \
    && apt-get clean

RUN sudo ldconfig

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get -y install \
        msodbcsql17 \
        libgssapi-krb5-2 \
        unixodbc-dev \
    && apt-get clean  


RUN pip3 install --upgrade pip && \
    pip3 install selenium \
        pandas \
        sqlalchemy \
        pyodbc