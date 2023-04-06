FROM ghcr.io/osgeo/gdal:ubuntu-small-latest AS builder

ENV TZ=Etc/UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  build-essential git wget zlib1g-dev libsqlite3-dev \
  && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/felt/tippecanoe \
 && cd tippecanoe \
 && make -j \
 && make install \
 && cd ../ \
 && rm -rf tippecanoe

FROM ghcr.io/osgeo/gdal:ubuntu-small-latest

ENV TZ=Etc/UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  libffi-dev python3-pip\
  && rm -rf /var/lib/apt/lists/* \
  && pip3 install --no-cache-dir -r requirements.txt

COPY --from=builder /usr/local/bin/tippecanoe /usr/local/bin/tippecanoe

COPY main.py ./ 
COPY ingest ./ingest

CMD [ "python", "main.py" ]