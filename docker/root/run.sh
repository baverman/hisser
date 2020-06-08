#!/bin/sh
exec uwsgi --ini /conf/uwsgi.ini --http $GRAPHITE_BIND
