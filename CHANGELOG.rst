0.20
====

* [Optimization] Huge performance improvements. Aggregations implemented via custom C-module.
  Other often used functions rewritten using numpy.

* [Update] Update base image to alpine:17 (python 3.10). Use numpy package from alpine repository
  instead of compiling it with `pip install ...`

* [Update] lmdb, cffi, uwsgi.

* [Feature] Sentry integration via SENTRY_DSN envvar.


0.17
====

* [Feature] Support for template vars in the ``alias`` function. One can refer to tags and name
  parts using ``{tag}`` and ``{0}`` placeholders.

* [Optimization] Use own tag parser. Skip all redundant checks.

* [Update] Update graphite to 1.17

* [Fix] Hisser must use lower resolution if there is no corresponding data.
