import re
from fnmatch import filter as fnfilter, translate

import lmdb
from .utils import MB, txn_cursor, make_key


class MetricIndex:
    def __init__(self, path, map_size=3000*MB):
        self.env = lmdb.open(path, map_size, subdir=False, max_dbs=6)
        self.env.reader_check()
        self.tree_db = self.env.open_db(b'tree', dupsort=True)
        self.tag_db = self.env.open_db(b'tag', dupsort=True)
        self.tag_name_db = self.env.open_db(b'tag_name')
        self.tag2idx_db = self.env.open_db(b'tag2idx')
        self.idx2tag_db = self.env.open_db(b'idx2tag')
        self.tag_cache = {}

    def idx2name(self, idx, ct):
        try:
            return self.tag_cache[idx]
        except KeyError:
            pass
        t = ct.get(idx)
        if t.startswith(b'name='):
            t = t[5:]
        self.tag_cache[idx] = t
        return t

    def decode_name(self, data, ct, start=0):
        name = self.idx2name
        return b';'.join(name(data[r:r+4], ct) for r in range(start, len(data), 4))

    def add(self, names):
        tagged = [r for r in names if b';' in r]
        simple = [r for r in names if b';' not in r]

        if simple:
            tree = make_tree(simple)
            with txn_cursor(self.env, True, self.tree_db) as cur:
                cur.putmulti(tree)

        if tagged:
            self.add_tags(tagged)

    def add_tags(self, names):
        parted_names = list(tag_parts(names))
        tags = set()
        for parts in parted_names:
            tags.update(parts)

        ids = self.get_tag_ids(tags)
        tag_names = []
        for parts in parted_names:
            nk = b''.join(ids[r] for r in parts)
            tag_names.extend((ids[r] + nk, b'') for r in parts)

        with txn_cursor(self.env, True, self.tag_name_db) as cur:
            cur.putmulti(tag_names, overwrite=False)

    def get_tag_ids(self, tags):
        tag_idx = {}
        with txn_cursor(self.env, True, self.tag2idx_db) as cur:
            for t in sorted(tags):
                tid = cur.get(t)
                if tid:
                    tag_idx[t] = tid
            unassigned_tags = tags.difference(tag_idx)

            if unassigned_tags:
                last_idx = int(str(cur.get(b'__last_idx__') or b'0', 'ascii'))
                cur.put(b'__last_idx__', str(last_idx + len(unassigned_tags)).encode())
                ti = [(t, idx.to_bytes(4, 'big'))
                      for idx, t in enumerate(unassigned_tags, last_idx+1)]
                cur.putmulti(ti)
                tag_idx.update(ti)

        if unassigned_tags:
            with txn_cursor(self.env, True, self.idx2tag_db) as cur:
                cur.putmulti((idx, t) for t, idx in ti)

            with txn_cursor(self.env, True, self.tag_db) as cur:
                g = (r.partition(b'=') for r in sorted(unassigned_tags))
                cur.putmulti(((k, v) for k, _, v in g))

        return tag_idx

    def iter_tree(self):
        with txn_cursor(self.env, False, self.tree_db) as cur:
            for k, v in cur:
                yield k, v

    def iter_tags(self):
        with txn_cursor(self.env, False, self.tag_db) as cur:
            for k, v in cur:
                yield k, v

    def iter_tag_names(self):
        name = self.idx2name
        with txn_cursor(self.env, False,
                        self.tag_name_db, self.idx2tag_db) as (cn, ct):
            for k in cn.iternext(True, False):
                yield (name(k[:4], ct), self.decode_name(k, ct, 4))

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
        with txn_cursor(self.env, False,
                        self.tag_name_db, self.tag2idx_db) as (cn, cti):
            for value in values:
                idx = cti.get('{}={}'.format(tag, value).encode())
                if idx and cn.set_range(idx):
                    for k in cn.iternext(True, False):
                        if idx == k[:4]:
                            result.append(k[4:])
                        else:
                            break
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

        if result:
            with txn_cursor(self.env, False, self.idx2tag_db) as ct:
                result = [self.decode_name(it, ct) for it in result]
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


def tag_parts(names):
    for name in names:
        parts = name.split(b';')
        if not parts:  # pragma: no cover
            continue

        yield (b'name=%s' % parts[0], *parts[1:])


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
