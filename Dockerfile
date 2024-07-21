## Base Image
FROM debian:bookworm-slim

## Changing workdir
WORKDIR /usr/src/alpha_library

## Adding fetch_and_run.sh
ADD fetch_and_run.sh /usr/local/bin/fetch_and_run.sh

## Default Packages
RUN apt update && \
    apt upgrade -y &&  \
    apt install libcurl4-openssl-dev libssl-dev curl -y &&\
    apt install python3-pip -y

# Needed for pycurl
ENV PYCURL_SSL_LIBRARY=openssl

## Add requirements first
COPY requirements.txt requirements.txt

## Install python packages
RUN pip3 install wheel setuptools pip --upgrade --break-system-packages && \
    pip3 install -r requirements.txt --break-system-packages

## Model for Language Detection
RUN curl "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.ftz" -o "/tmp/lid.176.ftz"

## Add code into the docker
COPY . .

## Entrypoint for docker
# ENTRYPOINT ["sh", "/usr/local/bin/fetch_and_run.sh"]
