import ujson
import os.path
import sqlite3
import functools
import logging

log = logging.getLogger('hisser.metrics')

EXT_PATH = os.path.abspath(os.path.dirname(__file__))


@functools.lru_cache()
def _get_connection(uri, _key):
    conn = sqlite3.connect(uri)
    conn.isolation_level = None
    conn.execute('pragma journal_mode=wal')
    conn.execute('pragma cache_size=-100000')
    conn.execute('pragma temp_store=2')  # temp tables in memory
    conn.enable_load_extension(True)
    try:
        conn.load_extension(os.path.join(EXT_PATH, 'regexp'))
    except:
        log.exception('Unable to load regexp sqlite extension')
    return conn


def get_connection(uri):
    return _get_connection(uri, os.getpid())


def qhelper(fields):
    if len(fields) == 1:
        wfield = f'{fields[0]}'
        json_extract = 'data.value'
    else:
        wfield = '({})'.format(', '.join(fields))
        json_extract = ', '.join(f"json_extract(data.value, '$[{i}]')" for i in range(len(fields)))

    return json_extract, wfield


class MetricIndex:
    def __init__(self, path):
        self.conn = get_connection(path)
        create_tables(self.conn)

    def get_existing(self, table, fields, values, rfields):
        extract, wfield = qhelper(fields)
        q = f'SELECT {", ".join(rfields)} FROM {table} WHERE {wfield} IN (SELECT {extract} FROM json_each(?) data)'
        return self.conn.execute(q, (ujson.dumps(values),)).fetchall()

    def insert(self, table, fields, values, rfields=None, ignore=False):
        extract, _ = qhelper(fields)
        q = f'''\
            INSERT {'OR IGNORE ' if ignore else ''} INTO {table} ({', '.join(fields)})
            SELECT {extract} FROM json_each(?) data
        '''
        if rfields:
            q += f" RETURNING {', '.join(rfields)}"

        return self.conn.execute(q, (ujson.dumps(values),)).fetchall()

    def add(self, names):
        dictfn = lambda data: {(it[0], it[1]): it[2] for it in data}

        names = [it.decode() for it in names]
        name_ids = dict(self.insert('metrics', ['name'], names,
                               rfields=['name', 'metric_id'], ignore=True))

        tagged_names = list(split_names(name_ids))
        tag2mid = {}
        for name, tlist in tagged_names:
            for it in tlist:
                t, _, v = it.partition('=')
                tag2mid.setdefault((t, v), []).append(name_ids[name])

        tag_ids = dictfn(self.get_existing('tags', ['tag', 'value'], list(tag2mid),
                                           rfields=['tag', 'value', 'tag_id']))
        tag_ids.update(dictfn(self.insert('tags', ['tag', 'value'], list(set(tag2mid) - set(tag_ids)),
                                          rfields=['tag', 'value', 'tag_id'])))

        mt = [(mid, tag_ids[tv]) for tv, mids in tag2mid.items() for mid in mids]
        self.insert('metric_tags', ['metric_id', 'tag_id'], mt)

    def find_metrics_many(self, queries):
        result = {}
        for q in queries:
            result[q] = self.find_metrics(q)
        return result

    def find_metrics(self, query):
        queries = []
        for idx, part in enumerate(query.split('.')):
            if '*' in part or '[' in part:
                queries.append(('.{}'.format(idx), '=~', '!' + part))
            else:
                queries.append(('.{}'.format(idx), '=', part))

        return self.match_by_tags(queries)

    def find_tree(self, query):
        sresult = self.find_metrics(query)
        slen = len(query.split('.'))
        result = []
        uset = set()
        for it in sresult:
            parts = it.split(b'.')
            if len(parts) == slen:
                result.append((True, it))
            elif len(parts) > slen:
                p = b'.'.join(parts[:slen])
                if p not in uset:
                    uset.add(p)
                    result.append((False, p))
        return result

    def pattern_match(self, pattern, negate=False):
        neg = 'NOT' if negate else ''
        if pattern.startswith(':'):
            values = [it for it in pattern[1:].split(',') if it]
            qs = ','.join('?' * len(values))
            return f'value {neg} IN ({qs})', values
        elif pattern.startswith('!'):
            return f'value {neg} GLOB ?', (pattern[1:],)
        else:
            return f'value {neg} REGEXP ?', (pattern,)

    def match_by_tags(self, queries, cache=None):
        cte_where = []
        values = []

        # dirty hack
        tag_priority = {'filename': 1, 'name': 3, 'status': 2, 'dc': 999}
        queries.sort(key=lambda x: (tag_priority.get(x[0], 99)))

        for tag, op, value in queries:
            if op == '=':
                cte_where.append('value = ?')
                values.extend((tag, value))
            elif op == '!=':
                cte_where.append('value != ?')
                values.extend((tag, value))
            elif op == '=~':
                q, v = self.pattern_match(value)
                cte_where.append(q)
                values.append(tag)
                values.extend(v)
            elif op == '!=~':
                q, v = self.pattern_match(value, True)
                cte_where.append(q)
                values.append(tag)
                values.extend(v)
            else:  # pragma: no cover
                continue

        assert cte_where, 'Empty query'

        idx = list(range(1, len(cte_where)+1))

        cte_list = ',\n'.join(f't{i} as (select tag_id from tags where tag = ? and {w})'
                              for i, w in enumerate(cte_where, 1))
        join_list = '\n'.join(f'        inner join metric_tags m{i} using (metric_id)'
                              for i in idx[1:])
        where_list = ' and '.join(f'm{i}.tag_id in t{i}' for i in idx)

        q = f'''\
            with {cte_list}
            select name
            from (
                select m1.metric_id
                from metric_tags m1
                {join_list}
                where {where_list}
            ) inner join metrics using (metric_id)
        '''

        # eq = 'EXPLAIN QUERY PLAN ' + q
        # for row in self.conn.execute(eq, values).fetchall():
        #     print(row)

        return sorted(set(it.encode() for it, in self.conn.execute(q, values).fetchall()))

    def iter_tags(self):
        q = 'select tag, value from tags order by tag, value'
        for tag, value in self.conn.execute(q):
            yield tag.encode(), value.encode()

    def get_tags(self):
        q = 'select distinct(tag) t from tags order by t'
        return [it.encode() for it, in self.conn.execute(q).fetchall()]

    def get_tag_values(self, tag):
        q = 'select value from tags where tag = ? order by value'
        return [it.encode() for it, in self.conn.execute(q, (tag,)).fetchall()]

    def iter_names(self):
        q = 'select name from metrics order by name'
        for name, in self.conn.execute(q):
            yield name.encode()


def split_names(names):
    for name in names:
        if ';' in name:
            parts = name.split(';')
            yield name, ('name=' + parts[0], *parts[1:])
        else:
            yield name, ['.{}={}'.format(*it) for it in enumerate(name.split('.'))]


def create_tables(conn):
    version = conn.execute("pragma user_version").fetchone()[0]
    if version == 0:
        with conn:
            conn.execute('CREATE TABLE metrics (metric_id INTEGER PRIMARY KEY, name TEXT UNIQUE)')
            conn.execute('CREATE TABLE tags (tag_id INTEGER PRIMARY KEY, tag TEXT, value TEXT)')
            conn.execute('CREATE UNIQUE INDEX ix_tags ON tags (tag, value)')
            conn.execute('CREATE TABLE metric_tags (metric_id INTEGER, tag_id INTEGER, PRIMARY KEY (tag_id, metric_id))')
            conn.execute('pragma user_version = 1')
