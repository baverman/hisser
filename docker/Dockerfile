# Base image with os requirements
# ===============================
FROM alpine:3.17 as base

ENV LANG=C.UTF-8
RUN apk add --no-cache python3 libffi cairo openssl ca-certificates pcre wget sqlite py3-pip py3-numpy


# Build tools, dependencies and python packages
# =============================================
FROM base as deps

RUN apk add python3-dev libffi-dev openssl-dev pcre-dev build-base linux-headers

ENV GRAPHITE_NO_PREFIX=True \
    PYTHONPATH=/pypkg/lib/python3.10/site-packages

RUN pip install --prefix=/pypkg --no-binary=:all: cffi

COPY /requirements.txt /
RUN pip install --prefix=/pypkg -r requirements.txt
COPY /hisser /pkg/hisser/
COPY README.rst LICENSE setup.py /pkg/
RUN pip install --prefix=/pypkg /pkg


# Final image
# ===========
FROM base

ENV PATH=/pypkg/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    PYTHONPATH=/conf:/pypkg/lib/python3.10/site-packages \
    GRAPHITE_ROOT=/data \
    GRAPHITE_STORAGE_DIR=/data \
    GRAPHITE_BIND=:8080 \
    HISSER_DATA_DIR=/data \
    HISSER_LOGGING_LEVEL=INFO

EXPOSE 2003 8080

COPY root /
COPY --from=deps /pypkg /pypkg

CMD /run.sh
