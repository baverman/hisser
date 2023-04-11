#!/bin/sh
ulimit -n 16384
exec uwsgi --ini /conf/uwsgi.ini --http $GRAPHITE_BIND
