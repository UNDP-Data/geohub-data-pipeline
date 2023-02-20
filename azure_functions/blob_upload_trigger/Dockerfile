FROM mcr.microsoft.com/azure-functions/python:4-python3.9-slim
ENV TZ=Etc/UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  build-essential ca-certificates git wget zlib1g-dev libsqlite3-dev \
  python3-pip \
  && rm -rf /var/lib/apt/lists/*

RUN pip3 install --upgrade pip

RUN git clone https://github.com/felt/tippecanoe \
  && cd tippecanoe \
  && make -j \
  && make install \
  && cd ../ \
  && rm -rf tippecanoe

WORKDIR /usr/src/app

RUN apt-get update \
    && apt-get install -y gdal-bin libgdal-dev

COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

COPY blob_upload_pipeline ./blob_upload_pipeline

CMD ["python3", "-m", "blob_upload_pipeline.__init__"]