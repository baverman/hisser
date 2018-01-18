from fnmatch import filter as fnfilter
from struct import Struct


import lmdb
from .utils import MB, txn_cursor

bigint_st = Struct('!Q')


class MetricNames:
    def __init__(self, path, map_size=500*MB):
        self.env = lmdb.open(path, map_size, subdir=False, max_dbs=2)
        self.names_db = self.env.open_db(b'names')
        self.ids_db = self.env.open_db(b'ids')

    def txn_allocate_ids(self, txn, count):
        if count <= 0:
            return None

        v = txn.get(b'__id_counter__')
        if v is None:
            v = 0
        else:
            v = bigint_st.unpack(v)[0]

        txn.put(b'__id_counter__', bigint_st.pack(v + count))
        return v + 1

    def allocate_ids(self, count):
        with self.env.begin(write=True) as txn:
            return self.txn_allocate_ids(txn, count)

    def txn_get_new_names(self, txn, names):
        with txn.cursor(self.names_db) as cur:
            return [r for r in names if cur.get(r) is None]

    def add(self, names, encoded=True):
        if not encoded:
            names = [r.encode() for r in names]

        with self.env.begin(write=True) as txn:
            names = self.txn_get_new_names(txn, names)
            if not names:
                return {}

            start_id = self.txn_allocate_ids(txn, len(names))
            result = {r: bigint_st.pack(i) for i, r in enumerate(names, start_id)}

            with txn.cursor(self.names_db) as cur:
                cur.putmulti(result.items())

            with txn.cursor(self.ids_db) as cur:
                cur.putmulti((v, k) for k, v in result.items())

        return result

    def get_ids(self, names):
        with txn_cursor(self.env, db=self.names_db) as cur:
            return [cur.get(r) for r in names]

    def get_names(self, ids):
        with txn_cursor(self.env, db=self.ids_db) as cur:
            return [cur.get(r) for r in ids]


class MetricIndex:
    def __init__(self, path, map_size=500*MB):
        self.env = lmdb.open(path, map_size, subdir=False, max_dbs=2)
        self.term_db = self.env.open_db(b'terms', dupsort=True)
        self.tree_db = self.env.open_db(b'tree', dupsort=True)

    def add(self, names):
        with txn_cursor(self.env, True, self.term_db) as cur:
            parts = group_parts(names)
            cur.putmulti(parts)

    def iter_terms(self):
        with txn_cursor(self.env, False, self.term_db) as cur:
            for k, v in cur:
                yield k, v

    def add_tree(self, names):
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
                    matched_metrics[q] = [(not cur.set_key(c), c) for c in candidates]
                else:
                    matched_metrics[q] = candidates
            return matched_metrics

    def find_metrics(self, query):
        return self.find_metrics_many([query]).get(query, [])

    def find_tree(self, query):
        return self.find_metrics_many([query], True).get(query, [])


def group_parts(names):
    result = []
    for name, nid in names.items():
        for i, p in enumerate(name.split(b'.')):
            prefix = bytes((i + 49,))
            result.append((prefix + p, nid))
    result.sort()
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


if __name__ == '__main__':
    import sys, time
    from .db import dump
    # names = {k: bigint_st.pack(i) for i, (k, v) in enumerate(dump(sys.argv[1]))}
    # for r in group_parts(names):
    #     print(r)
    mi = MetricIndex('tmp/metric.index')
    # for fname in sys.argv[1:]:
    #     names = (k for k, v in dump(fname))
    #     mi.add_tree(names)
    t = time.time()
    for r in mi.find_tree(sys.argv[1]):
        print(r)
    print(time.time() - t)
