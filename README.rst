hisser
======

|travis| |coverage| |pyver|

.. |travis| image:: https://travis-ci.org/baverman/hisser.svg?branch=master
   :target: https://travis-ci.org/baverman/hisser

.. |coverage| image:: https://img.shields.io/badge/coverage-100%25-brightgreen.svg

.. |pyver| image:: https://img.shields.io/badge/python-3.5%2C_3.6-blue.svg


Time series database, backend for graphite, fast alternative to carbon + whisper

All metrics are dumped into blocks (lmdb databases). Each block
contain all metrics and their data for particular amount of time.

::

    data_dir
    ├── resolution1
    │   ├── block1.hdb     # block data
    │   ├── block2.hdb
    │   └── blocks.state   # notifications
    ├── resolution2
    │   ├── block1.hdb
    │   ├── block2.hdb
    │   └── blocks.state
    └── metric.index       # metric name index


::

    -------------------------------------------------------------->  time
     +------------------------------+------------+----+----+----+
     |                              |            |    |    |    |
     |            block1            |   block2   | b3 | b4 | b5 |
     |                              |            |    |    |    |
     +------------------------------+------------+----+----+----+

                                ||  periodic
                                \/  merge

     +------------------------------+----------------------+----+
     |                              |                      |    |
     |            block1            |         block2'      | b5 |
     |                              |                      |    |
     +------------------------------+----------------------+----+
     |                                                     /
     |   ||  periodic downsample            ---------------
     |   \/                                /
     |          /--------------------------
     +---------+
     |         |
     | block1' |
     |         |
     +---------+
