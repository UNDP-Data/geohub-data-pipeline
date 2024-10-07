# Build felt/tippecanoe
# Dockerfile from https://github.com/felt/tippecanoe/blob/main/Dockerfile
# add "--platform=linux/x86_64" for M1 Mac
FROM ubuntu:22.04 AS tippecanoe-builder

RUN apt-get update \
  && apt-get -y install build-essential libsqlite3-dev zlib1g-dev git

RUN git clone https://github.com/felt/tippecanoe
WORKDIR tippecanoe
RUN make

# Build production docker image
# add "--platform=linux/amd64" for M1 Mac
FROM ghcr.io/osgeo/gdal:ubuntu-small-3.8.5

ENV TZ=Etc/UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  libffi-dev python3-pip libsqlite3-0 \
  && rm -rf /var/lib/apt/lists/* \
  && pip3 install --no-cache-dir -r requirements.txt

# copy tippecanoe to production docker image
COPY --from=tippecanoe-builder /tippecanoe/tippecanoe* /usr/local/bin/
COPY --from=tippecanoe-builder /tippecanoe/tile-join /usr/local/bin/

COPY main.py ./ 
COPY ingest ./ingest

CMD [ "python", "main.py" ]