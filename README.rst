Hisser
======

|travis| |coverage| |pyver|

.. |travis| image:: https://travis-ci.org/baverman/hisser.svg?branch=master
   :target: https://travis-ci.org/baverman/hisser

.. |coverage| image:: https://img.shields.io/badge/coverage-100%25-brightgreen.svg

.. |pyver| image:: https://img.shields.io/badge/python-3.5%2C_3.6-blue.svg


Time series database, backend for graphite, fast alternative to carbon + whisper.

Features:

* Low disk usage (IOPS) for metric store, it depends from actual data
  volumes instead of a number of metrics (in case of whisper). Hisser
  was designed to process million of metrics.

* Fast queries. Optimized query parsing and response rendering (~3x
  boost comparing with vanilla graphite-web).

* Tag support.

* Drop-in replacement for whisper + carbon.

* Smart ``alias`` function which can expand ``{tag}`` and ``{0}`` (name part)
  variables.

* 100% test coverage.


.. contents:: **Table of Contents**


Configuration
-------------

Default options and documentation for them can be read in
`default config`_.

.. _default config: hisser/defaults.py

You can create custom configuration file and use ``--config`` cli option or
use ``HISSER_*`` environment variables to override default values.
For example ``HISSER_DATA_DIR`` will set ``DATA_DIR`` configuration
parameter.


Run
---

Simplest way is to use official `docker image`::

   docker run --rm -u $(id -u):$(id -g) -p 2003:2003 -p 8080:8080 -v /path/to/data:/data baverman/graphite-hisser

Port `2003` is a graphite protocol. `8080` is graphite API, you can point
grafana to it. In production you don't need 8080 port accessible from
external network. In this case you should use separate docker network
and map 2003 port only or use ``--network host`` and specify ``GRAPHITE_BIND=127.0.0.1:8080``
envvar.

IMPORTANT! To use tag support with grafana you need grafana 5.x and set graphite
version ``1.1.x`` in storage settings.

Note: for grafana you can use tiny `grafana image`_.

.. _docker image: https://hub.docker.com/r/baverman/graphite-hisser/
.. _grafana image: https://hub.docker.com/r/baverman/grafana/


Internals
---------

Hisser is a very simple metric storage. All heavy work is done by `lmdb`_.
Metrics are organized into blocks (lmdb databases). Each block
contains all metrics and their data for particular amount of time. Blocks
with same resolution are grouped under corresponding directory:

Example data layout:

::

   data_dir/
   ├── 300  # resolution (1 data point every 5-minute)
   │   ├── 1533990300.519.hdb   # timestamp-of-block-start.number-of-points.hdb
   │   ├── 1534621800.191.hdb
   │   ├── 1534679100.48.hdb
   │   └── blocks.state         # lock file
   ├── 60   # resolution (1 data point every minute)
   │   ├── 1534621920.700.hdb
   │   ├── 1534663920.320.hdb
   │   ├── 1534683120.160.hdb
   │   ├── 1534692720.40.hdb
   │   ├── 1534695120.11.hdb
   │   ├── 1534695900.6.hdb
   │   └── blocks.state
   └── metric.index       # metric name and tag index


This layout allows to dump data from memory buffer very efficiently (whisper
needs one io-operation per metric and can kneel a host with several hundreds of
metrics).

If points in memory exceed ``BUFFER_FLUSH_SIZE`` or ``BUFFER_MAX_POINTS`` it will be
flushed into separate block::

   +----------+----------+----------+
   |  block1  |  block2  |  block3  |  resolution 60
   +----------+----------+----------+

From time to time small blocks are merged into greater one::

   +---------------------+----------+
   |       block12       |  block3  |  resolution 60
   +---------------------+----------+

And from time to time big blocks are downsampled into blocks with lower
resolution::

   +---------------------+----------+
   |       block12       |  block3  |  resolution 60
   +---------------------+----------+
              |
              v
        +----------+
        | block12' |  resolution 300
        +----------+

Yes, it is very simple.

.. _lmdb: http://www.lmdb.tech/doc/


FAQ
---

1. But there is a better alternative to whisper. InfluxDB!

   Yes, InfluxDB is a way better than whisper. But is has some drawbacks
   comparing to hisser.

   * Requires more data space.
   * Consumes more IOPS, memory and CPU.
   * Needs manual retention configuration.
   * Slower to query.
   * Implicit metric grouping can lead to confusing graphs in grafana.
     You have to limit groups to explicit tag values or do ``group by
     $tag``.
