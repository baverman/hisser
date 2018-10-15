# Base image with os requirements
# ===============================
FROM alpine:3.8 as base

ENV LANG=C.UTF-8
RUN apk update \
 && apk add python3 libffi cairo libressl ca-certificates pcre wget sqlite


# Build tools, dependencies and python packages
# =============================================
FROM base as deps

RUN apk add python3-dev libffi-dev libressl-dev pcre-dev build-base linux-headers

ARG GRAPHITE_VERSION
ENV GRAPHITE_NO_PREFIX=True \
    PYTHONPATH=/pypkg/lib/python3.6/site-packages
# ENV CFLAGS='-D MDB_MAXKEYSIZE=1023'

RUN pip3 install --prefix=/pypkg uwsgi==2.0.17 ujson==1.35 graphite-web==$GRAPHITE_VERSION \
                 click==6.7 lmdb==0.94 msgpack==0.5.6 xxhash==1.2.0
COPY /hisser /pkg/hisser/
COPY README.rst LICENSE setup.py /pkg/
RUN pip3 install --prefix=/pypkg /pkg


# Final image
# ===========
FROM base

ENV PATH=/pypkg/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    PYTHONPATH=/conf:/pypkg/lib/python3.6/site-packages \
    GRAPHITE_ROOT=/data \
    GRAPHITE_STORAGE_DIR=/data \
    DJANGO_SETTINGS_MODULE=graphite_local_settings \
    GRAPHITE_BIND=:8080 \
    HISSER_DATA_DIR=/data \
    HISSER_LOGGING_LEVEL=INFO

EXPOSE 2003 8080

COPY root /
COPY --from=deps /pypkg /pypkg

CMD /run.sh
