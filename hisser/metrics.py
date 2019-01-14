import re
from fnmatch import translate

import lmdb
from .utils import MB, txn_cursor, make_key, cached_property


class MetricIndex:
    def __init__(self, path, map_size=3000*MB):
        self.path = path
        self.map_size = map_size

    @cached_property
    def env(self):
        env = lmdb.open(self.path, self.map_size, subdir=False,
                        max_dbs=7, max_readers=4096)
        env.reader_check()
        return env

    @cached_property
    def tag_values_db(self):
        return self.env.open_db(b'tag:value', dupsort=True)

    @cached_property
    def tag_ids_db(self):
        return self.env.open_db(b'tag=value:tag_id')

    @cached_property
    def tag_ids_rev_db(self):
        return self.env.open_db(b'tag_id:tag=value')

    @cached_property
    def tag_name_db(self):
        return self.env.open_db(b'tag_id:name_id', dupsort=True)

    @cached_property
    def name_tags_db(self):
        return self.env.open_db(b'name_id:tag_ids')

    @cached_property
    def name_hashes_db(self):
        return self.env.open_db(b'name_hash:')

    def filter_existing_names(self, names):
        with txn_cursor(self.env, True, self.name_hashes_db) as cur:
            result = [it for it in names if cur.get(make_key(it)) is None]
        return result

    def add(self, names):
        tagged_names = list(split_names(self.filter_existing_names(names)))
        if not tagged_names:
            return
        last_name_id = self.alloc_id(b'last_name_id', len(tagged_names))

        tags = set()
        for _, parts in tagged_names:
            tags.update(parts)

        ids = self.get_tag_ids(tags)

        tag_names = []
        names = []
        for _, parts in tagged_names:
            last_name_id += 1
            nid = last_name_id.to_bytes(4, 'big')
            tag_names.extend((ids[r], nid) for r in parts)
            names.append((nid, b''.join(ids[r] for r in parts)))

        with txn_cursor(self.env, True, self.tag_name_db) as cur:
            cur.putmulti(tag_names)

        with txn_cursor(self.env, True, self.name_tags_db) as cur:
            cur.putmulti(names)

        with txn_cursor(self.env, True, self.name_hashes_db) as cur:
            cur.putmulti((make_key(it[0]), b'') for it in tagged_names)

    def alloc_id(self, name, count=1):
        with txn_cursor(self.env, True, None) as cur:
            last_id = int((cur.get(name) or b'0').decode())
            cur.put(name, str(last_id + count).encode())
        return last_id

    def get_tag_ids(self, tags):
        tag_ids = {}
        with txn_cursor(self.env, True, self.tag_ids_db) as cur:
            for t in sorted(tags):
                tid = cur.get(t)
                if tid:
                    tag_ids[t] = tid
            unassigned_tags = tags.difference(tag_ids)

        if unassigned_tags:
            last_id = self.alloc_id(b'last_tag_id', len(unassigned_tags))
            ti = [(t, idx.to_bytes(4, 'big'))
                  for idx, t in enumerate(unassigned_tags, last_id+1)]
            tag_ids.update(ti)
            with txn_cursor(self.env, True, self.tag_ids_db) as cur:
                cur.putmulti(ti)

            with txn_cursor(self.env, True, self.tag_ids_rev_db) as cur:
                cur.putmulti((tid, t) for t, tid in ti)

            g = (r.partition(b'=') for r in sorted(unassigned_tags))
            with txn_cursor(self.env, True, self.tag_values_db) as cur:
                cur.putmulti(((k, v) for k, _, v in g))

        return tag_ids

    def iter_names(self):  # pragma: no cover
        cache = {}
        with txn_cursor(self.env, False, self.name_tags_db,
                        self.tag_ids_rev_db) as (cn, ct):
            for tag_ids in cn.iternext(False, True):
                yield self.decode_name(tag_ids, ct, cache)

    def decode_name_tagged(self, data, ct, cache):
        name = self.idx2name_tagged
        return b';'.join(name(data[r:r+4], ct, cache)
                         for r in range(0, len(data), 4))

    def decode_name(self, data, ct, cache):
        name = self.idx2name
        return b'.'.join(name(data[r:r+4], ct, cache)
                         for r in range(0, len(data), 4))

    def idx2name_tagged(self, idx, ct, cache):
        try:
            return cache[idx]
        except KeyError:
            pass
        t = ct.get(idx)
        if t.startswith(b'name='):
            t = t[5:]
        cache[idx] = t
        return t

    def idx2name(self, idx, ct, cache):
        try:
            return cache[idx]
        except KeyError:
            pass
        t = ct.get(idx).partition(b'=')[2]
        cache[idx] = t
        return t

    def iter_tags(self):
        with txn_cursor(self.env, False, self.tag_values_db) as cur:
            for k, v in cur:
                yield k, v

    def get_tags(self):
        with txn_cursor(self.env, False, self.tag_values_db) as cur:
            return list(cur.iternext_nodup(True, False))

    def get_tag_values(self, tag):
        with txn_cursor(self.env, False, self.tag_values_db) as cur:
            if cur.set_key(tag.encode()):
                return list(cur.iternext_dup(False, True))
        return []

    def pattern_match(self, pattern, tag, cache):
        if pattern.startswith(':'):
            return [it.encode() for it in pattern[1:].split(',') if it]
        elif pattern.startswith('!'):
            values = self.cached_tag_values(tag, cache)
            m = re.compile(translate(pattern[1:]).encode()).match
            return [r for r in values if m(r)]
        else:
            values = self.cached_tag_values(tag, cache)
            m = re.compile(pattern.encode()).match
            return [r for r in values if m(r)]

    def pattern_not_match(self, pattern, tag, cache):
        values = self.cached_tag_values(tag, cache)
        if pattern.startswith(':'):
            enum = set(it.encode() for it in pattern[1:].split(',') if it)
            return [r for r in values if r not in enum]
        elif pattern.startswith('!'):
            m = re.compile(translate(pattern[1:]).encode()).match
            return [r for r in values if not m(r)]
        else:
            m = re.compile(pattern.encode()).match
            return [r for r in values if not m(r)]

    def find_metrics_many(self, queries, cache=None):
        result = {}
        for q in queries:
            result[q] = self.find_metrics(q, cache=cache)
        return result

    def find_tree(self, query):
        return self.find_metrics(query, False)

    def find_metrics(self, query, exact=True, cache=None):
        if cache is None:
            cache = {}

        queries = []
        for idx, part in enumerate(query.split('.')):
            if '*' in part or '[' in part:
                queries.append(('.{}'.format(idx), '=~', '!' + part))
            else:
                queries.append(('.{}'.format(idx), '=', part))

        name_ids = self._match_by_tags(queries, cache)
        if not name_ids:
            return []  # pragma: no cover

        name_ids = sorted(name_ids)
        name_datas = []
        with txn_cursor(self.env, False, self.name_tags_db,
                        self.tag_ids_rev_db) as (cn, ct):
            for it in name_ids:
                tag_data = cn.get(it)
                if tag_data:
                    name_datas.append(tag_data)

            elen = len(queries) * 4
            if exact:
                result = [self.decode_name(it, ct, cache)
                          for it in name_datas if len(it) == elen]
            else:
                is_root = {}
                uniq = []
                uset = set()
                for it in name_datas:
                    tit = it[:elen]
                    is_root[tit] = len(it) == elen
                    if tit not in uset:
                        uniq.append(tit)
                        uset.add(tit)

                result = [(is_root[it], self.decode_name(it, ct, cache))
                          for it in uniq]

        return result

    def match_by_tags(self, queries, cache=None):
        if cache is None:
            cache = {}

        name_ids = self._match_by_tags(queries, cache)
        if not name_ids:
            return []

        result = []
        name_ids = sorted(name_ids)
        with txn_cursor(self.env, False, self.name_tags_db,
                        self.tag_ids_rev_db) as (cn, ct):
            for it in name_ids:
                tag_data = cn.get(it)
                if tag_data:
                    result.append(
                        self.decode_name_tagged(tag_data, ct, cache))
        return result

    def _match_by_tags(self, queries, cache=None):
        tag_ids = []
        for tag, op, value in queries:
            if op == '=':
                values = [value.encode()]
            elif op == '!=':
                value = value.encode()
                values = [r for r in self.cached_tag_values(tag, cache)
                          if r != value]
            elif op == '=~':
                values = self.pattern_match(value, tag, cache)
            elif op == '!=~':
                values = self.pattern_not_match(value, tag, cache)
            else:  # pragma: no cover
                continue

            ids = self.get_tag_ids_by_values(tag.encode(), values)
            if not ids:
                return set()
            tag_ids.append(ids)

        name_ids = []
        with txn_cursor(self.env, False, self.tag_name_db) as cn:
            cursors = [TagIdCursor(cn, it[0])
                       if len(it) == 1
                       else MultyTagIdCursor(cn, it)
                       for it in tag_ids]
            cname = None
            counts = set()
            while True:
                for c in cursors:
                    if not cname:
                        cname = c.next(None)
                        if cname is None:  # pragma: no cover
                            return name_ids
                        counts.add(c)
                    else:
                        nname = c.next(cname)
                        if not nname:
                            return name_ids
                        if nname == cname:
                            counts.add(c)
                        else:
                            counts = set([c])
                            cname = nname

                if len(counts) == len(cursors):
                    name_ids.append(cname)
                    counts.clear()
                    cname = (int.from_bytes(cname, 'big') + 1).to_bytes(
                        4, 'big')

        return name_ids  # pragma: no cover

    def get_tag_ids_by_values(self, tag, values):
        with txn_cursor(self.env, False, self.tag_ids_db) as cti:
            return list(filter(
                None, (cti.get(b'%s=%s' % (tag, it)) for it in values)))

    def cached_tag_values(self, tag, cache):
        try:
            return cache[tag]
        except KeyError:
            pass
        result = cache[tag] = self.get_tag_values(tag)
        return result


class TagIdCursor:
    def __init__(self, cursor, tag_id):
        self.cursor = cursor
        self.tag_id = tag_id

    def next(self, name_id):
        c = self.cursor
        if name_id is None:
            if c.set_key(self.tag_id):
                return c.value()
            else:
                return None  # pragma: no cover
        else:
            if c.set_range_dup(self.tag_id, name_id):
                return c.value()
            else:
                return None


class MultyTagIdCursor:
    def __init__(self, cursor, tag_ids):
        self.cursor = cursor
        self.tag_ids = tag_ids
        self.min_names = None

    def next(self, name_id):
        c = self.cursor
        if not self.min_names:
            self.min_names = [c.get(it) for it in self.tag_ids]
        else:
            for idx, mn in enumerate(self.min_names):
                if mn is None:
                    continue
                if mn < name_id:
                    if c.set_range_dup(self.tag_ids[idx], name_id):
                        self.min_names[idx] = c.value()
                    else:
                        self.min_names[idx] = None

        return min(filter(None, self.min_names), default=None)


def split_names(names):
    for name in names:
        if b';' in name:
            parts = name.split(b';')
            yield name, (b'name=%s' % parts[0], *parts[1:])
        else:
            yield name, [b'.%d=%s' % it for it in enumerate(name.split(b'.'))]
