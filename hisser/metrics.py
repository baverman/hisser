from fnmatch import filter as fnfilter

import lmdb
from .utils import MB, txn_cursor


class MetricIndex:
    def __init__(self, path, map_size=500*MB):
        self.env = lmdb.open(path, map_size, subdir=False, max_dbs=2)
        self.tree_db = self.env.open_db(b'tree', dupsort=True)

    def add(self, names):
        tree = make_tree(names)
        with txn_cursor(self.env, True, self.tree_db) as cur:
            cur.putmulti(tree)

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
