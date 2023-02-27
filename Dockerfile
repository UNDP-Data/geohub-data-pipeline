FROM mcr.microsoft.com/azure-functions/python:4-python3.9-slim
ENV TZ=Etc/UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  build-essential ca-certificates git wget zlib1g-dev libsqlite3-dev \
  gdal-bin libgdal-dev python3-pip \
  && rm -rf /var/lib/apt/lists/*
# temporary only because compiling tp takes lots of time
#RUN git clone https://github.com/felt/tippecanoe \
#  && cd tippecanoe \
#  && make -j \
#  && make install \
#  && cd ../ \
#  && rm -rf tippecanoe

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

COPY main.py ./ 
COPY ingest ./ingest


CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]