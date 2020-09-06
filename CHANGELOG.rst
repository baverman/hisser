0.17
====

* [Feature] Support for template vars in the ``alias`` function. One can refer to tags and name
  parts using ``{tag}`` and ``{0}`` placeholders.

* [Optimization] Use own tag parser. Skip all redundant checks.

* [Update] Update graphite to 1.17

* [Fix] Hisser must use lower resolution if there is no corresponding data.
