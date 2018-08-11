import re
from fnmatch import filter as fnfilter, translate

import lmdb
from .utils import MB, txn_cursor


class MetricIndex:
    def __init__(self, path, map_size=500*MB):
        self.env = lmdb.open(path, map_size, subdir=False, max_dbs=4)
        self.tree_db = self.env.open_db(b'tree', dupsort=True)
        self.tag_name_db = self.env.open_db(b'tag_name', dupsort=True)
        self.tag_db = self.env.open_db(b'tag', dupsort=True)

    def add(self, names):
        tagged = [r for r in names if b';' in r]
        simple = [r for r in names if b';' not in r]

        if simple:
            tree = make_tree(simple)
            with txn_cursor(self.env, True, self.tree_db) as cur:
                cur.putmulti(tree)

        if tagged:
            tags, tag_names = parse_tags(tagged)
            with txn_cursor(self.env, True, self.tag_db) as cur:
                cur.putmulti(tags)
            with txn_cursor(self.env, True, self.tag_name_db) as cur:
                cur.putmulti(tag_names)

    def iter_tree(self):
        with txn_cursor(self.env, False, self.tree_db) as cur:
            for k, v in cur:
                yield k, v

    def find_metrics_many(self, queries, check=False):
        matched_metrics = {}
        with txn_cursor(self.env, False, self.tree_db) as cur:
            for q in queries:
                prefix, parts = query_parts(q)
                candidates = [prefix or b'.']
                for pattern in parts:
                    to_match = {}
                    for c in candidates:
                        if cur.set_key(c):
                            prefix = b'' if c == b'.' else c + b'.'
                            for m in cur.iternext_dup():
                                to_match.setdefault(m, []).append(prefix + m)

                    candidates[:] = []
                    for m in fnfilter(to_match, pattern):
                        candidates.extend(to_match[m])

                if check:
                    matched_metrics[q] = [(not cur.set_key(c), c) for c in sorted(candidates)]
                else:
                    matched_metrics[q] = sorted(candidates)
            return matched_metrics

    def find_metrics(self, query):
        return self.find_metrics_many([query]).get(query, [])

    def find_tree(self, query):
        return self.find_metrics_many([query], True).get(query, [])

    def find_by_tag_values(self, tag, values):
        result = []
        with txn_cursor(self.env, False, self.tag_name_db) as cur:
            for value in values:
                key = '{}={}'.format(tag, value).encode()
                if cur.set_key(key):
                    result.extend(cur.iternext_dup(False, True))
        return result

    def get_tags(self):
        with txn_cursor(self.env, False, self.tag_db) as cur:
            return list(cur.iternext_nodup(True, False))

    def get_tag_values(self, tag):
        with txn_cursor(self.env, False, self.tag_db) as cur:
            if cur.set_key(tag.encode()):
                return list(cur.iternext_dup(False, True))
        return []

    def match_by_tags(self, queries, cache=None):
        if cache is None:
            cache = {}

        result = None
        for tag, op, value in queries:
            if op == '=':
                values = [value]
            elif op == '!=':
                values = [r for r in self.cached_tag_values(tag, cache)
                          if r != value]
            elif op == '=~':
                values = pattern_match(self.cached_tag_values(tag, cache), value)
            elif op == '!=~':
                values = pattern_not_match(self.cached_tag_values(tag, cache), value)
            else:  # pragma: no cover
                continue

            names = self.find_by_tag_values(tag, values)
            if result is None:
                result = set(names)
            else:
                result.intersection_update(names)

            if not result:
                return set()

        return result

    def cached_tag_values(self, tag, cache):
        try:
            return cache[tag]
        except KeyError:
            pass
        result = cache[tag] = [r.decode() for r in self.get_tag_values(tag)]
        return result


def make_tree(names):
    empty_row = [None] * 255
    prev = empty_row[:]
    for n in names:
        prefix = None
        parts = n.split(b'.')
        for idx, (pp, p) in enumerate(zip(prev, parts)):
            if pp != p:
                prev[idx] = p
                prev[idx+1:255] = empty_row[:255-idx-1]
                yield (prefix or b'.', p)
            if prefix:
                prefix += b'.' + p
            else:
                prefix = p


def query_parts(query):
    parts = query.encode().split(b'.')
    prefix = []
    for q in parts:
        if any(r in q for r in (b'*', b'[', b']')):
            break
        else:
            prefix.append(q)
    return b'.'.join(prefix), parts[len(prefix):]


def parse_tags(names):
    tags = []
    tag_names = []
    for name in names:
        parts = name.split(b';')
        if not parts:  # pragma: no cover
            continue

        tags.append((b'name', parts[0]))
        tag_names.append((b'name=%b' % parts[0], name))

        for p in parts[1:]:
            k, _, v = p.partition(b'=')
            tags.append((k, v))
            tag_names.append((b'%b=%b' % (k, v), name))

    return tags, tag_names


def pattern_match(values, pattern):
    if pattern.startswith(':'):
        enum = set(filter(None, pattern[1:].split(',')))
        return [r for r in values if r in enum]
    elif pattern.startswith('!'):
        m = re.compile(translate(pattern[1:])).match
        return [r for r in values if m(r)]
    else:
        m = re.compile(pattern).match
        return [r for r in values if m(r)]


def pattern_not_match(values, pattern):
    if pattern.startswith(':'):
        enum = set(filter(None, pattern[1:].split(',')))
        return [r for r in values if r not in enum]
    elif pattern.startswith('!'):
        m = re.compile(translate(pattern[1:])).match
        return [r for r in values if not m(r)]
    else:
        m = re.compile(pattern).match
        return [r for r in values if not m(r)]
