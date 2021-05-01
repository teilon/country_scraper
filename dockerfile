FROM python:3.8-alpine

WORKDIR /app
COPY ./app /app
COPY ./requirements.txt /app

RUN apk add --no-cache \
        gcc \
        libxml2-dev \
        libxslt-dev \
        zlib-dev \
        libffi-dev \
        libressl-dev \
        musl-dev && \
    pip install -r requirements.txt && \
    apk del gcc musl-dev

# RUN pip install --no-cache-dir -r requirements.txt
# RUN pip install -r requirements.txt

ENV TZ Asia/Almaty
