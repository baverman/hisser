#!/usr/bin/env python3
import sys
import os.path
import glob
import re
import shlex
import subprocess
import errno
import string
from itertools import product
from functools import lru_cache
from time import time
from hashlib import md5

DEPS = {}
fmt = string.Formatter()

class Error(Exception): pass


def patsub(frompat, topat, items):
    frompat = frompat.replace('%', '(.+)')
    topat = topat.replace('%', r'\1')
    return [re.sub(frompat, topat, it) for it in items]


def allfiles(root):
    result = []
    for r, _, files in os.walk(root):
        result.extend(os.path.join(r, it) for it in files)
    return result


@lru_cache(None)
def makedirs(dirname):
    os.makedirs(dirname, exist_ok=True)


@lru_cache(None)
def get_mtime(fname):
    try:
        return os.path.getmtime(fname)
    except OSError as e:  # pragma: no cover
        if e.errno != errno.ENOENT:
            raise
        return 0


class fset(dict):
    def __init__(self, match, frompat, topat):
        if isinstance(match, (list, tuple, set)):
            self.src = match
        else:
            self.src = glob.glob(match, recursive=True)
        self.dest = patsub(frompat, topat, self.src)
        dict.__init__(self, zip(self.dest, self.src))
        assert not (set(self.src) & set(self.dest)), 'Source and dest files have similar items'


class Dep(object):
    def __init__(self):
        self.reqs = []
        self.deps = []
        self.order = []
        self.rule = None
        self.phony = False

    def iter_reqs(self):
        for r in self.reqs:
            yield r
        for r in self.deps:
            yield r
        for r in self.order:
            yield r


@lru_cache(None)
def parse_cmd(cmd):
    parts = shlex.split(cmd)
    result = []
    for p in parts:
        flist = []
        elist = []
        for prefix, expr, _spec, _cnv in fmt.parse(p):
            flist.append(prefix)
            if expr:
                flist.append('{}')
                elist.append(compile(expr, expr, 'eval'))
        result.append((''.join(flist), elist))
    return result


def eval_cmd(cmd, globs=None, locs=None):
    result = []
    for f, elist in parse_cmd(cmd):
        if not elist:
            result.append(f)
            continue

        vals = []
        for e in elist:
            vals.append(flatten(eval(e, globs, locs)))

        for va in product(*vals):
            result.append(f.format(*va))
    return result


def execute(cmd, globs=None, locs=None, depth=1):
    if not globs and not locs:
        frame = sys._getframe(depth)
        globs = frame.f_globals
        locs = frame.f_locals

    cmd = eval_cmd(cmd, globs, locs)
    subprocess.check_call(cmd)


class Rule(object):
    def __init__(self, cmd, params, depth=1, shell=False):
        if type(cmd) == str:
            cmd = [cmd]
        self.cmd = cmd
        self.params = params or {}
        self.globals = sys._getframe(depth).f_globals
        self.shell= shell

    def execute(self, target, dep):
        if callable(self.cmd):
            print(self.cmd.__name__, dep.reqs, '->', target)
            self.cmd(self, target, dep)
        else:
            l = {'target': target, 'reqs': dep.reqs,
                 'req': dep.reqs and dep.reqs[0]}
            l.update(self.params)
            for cmd in self.cmd:
                ecmd = eval_cmd(cmd, self.globals, l)
                print(' '.join(map(shlex.quote, ecmd)))
                if self.shell:
                    ecmd = ' '.join(map(shlex.quote, ecmd))
                subprocess.check_call(ecmd, shell=self.shell)

    def get_hash(self, target, dep):
        if callable(self.cmd):
            data = self.cmd.__code__.co_code
        else:
            l = {'target': target, 'reqs': dep.reqs,
                 'req': dep.reqs and dep.reqs[0]}
            l.update(self.params)
            data = []
            for cmd in self.cmd:
                data.append(' '.join(eval_cmd(cmd, self.globals, l)))
            data = '|'.join(data).encode()
        return md5(data).hexdigest()


class RuleHolder(object):
    def __init__(self, tmap, params, depth, shell):
        self.tmap = tmap
        self.params = params
        self.depth = depth
        self.shell = shell

    def __call__(self, fn):
        rule = Rule(fn, self.params, self.depth+1, self.shell)
        for t in self.tmap:
            assert not DEPS[t].rule
            DEPS[t].rule = rule
        return fn


def flatten(items):
    if type(items) in (str, bytes, dict) or not hasattr(items, '__iter__'):
        return [items]
    result = []
    for it in items:
        if type(it) in (list, tuple):
            result.extend(flatten(it))
        else:
            result.append(it)
    return result


def map_targets(targets):
    if type(targets) is str:
        targets = [targets]

    if type(targets) is list:
        targets = {it: [] for it in flatten(targets)}

    return {target: flatten(treqs) for target, treqs in targets.items()}


def get_dep(target):
    try:
        return DEPS[target]
    except KeyError:
        pass
    result = DEPS[target] = Dep()
    return result


def make(targets, reqs=None, cmd=None, deps=None, order=None,
         phony=None, _depth=1, shell=False, **params):
    rule = cmd and Rule(cmd, params, depth=_depth+1, shell=shell)
    tmap = map_targets(targets)

    areqs = []
    adeps = []
    aorder = []

    if reqs and reqs is not True:
        areqs = flatten(reqs)
    if deps and deps is not True:
        adeps = flatten(deps)
    if order and order is not True:
        aorder = flatten(order)

    for t, r in tmap.items():
        d = get_dep(t)
        if phony is not None:
            d.phony = phony

        # Select list to extend for target map reqs
        if deps is True:
            d.deps.extend(r)
        elif order is True:
            d.order.extend(r)
        else:
            d.reqs.extend(r)

        areqs and d.reqs.extend(areqs)
        adeps and d.deps.extend(adeps)
        aorder and d.order.extend(aorder)

        if rule:
            if d.rule:
                raise Error('Duplicate rule for {}'.format(t))
            d.rule = rule

    return RuleHolder(tmap, params, _depth, shell)


def iter_stale_leaves(nodes, seen, state):
    for node in nodes:
        if node in seen or node in state:
            return

        seen[node] = True
        dep = DEPS.get(node)
        if dep:
            possible_targets = []
            for r in dep.iter_reqs():
                if r in DEPS and r not in state:
                    possible_targets.append(r)

            # print(node, possible_targets)
            if possible_targets:
                yield from iter_stale_leaves(possible_targets, seen, state)
            else:
                yield node
        else:
            assert False, 'No way'  # pragma: no cover


def process_target(target, tstate, state, always_make):
    state[target] = 'processing'
    rstate = tstate or {}

    do = False
    dep = DEPS[target]
    direct = dep.reqs + dep.deps
    sub = [it for it in direct if it in DEPS]
    files = [it for it in direct if it not in DEPS or not DEPS[it].phony]
    src_files = [it for it in dep.iter_reqs() if it not in DEPS]

    for f in src_files:
        if not os.path.exists(f):
            raise Error(f'There is no dependency {f} to build {target}')

    if dep.phony or always_make:
        do = True

    if not do:
        do = any(state[it] == 'new' for it in sub)

    if dep.rule:
        if not do:
            tstamp = rstate.get('ts')
            do = not tstamp or any(get_mtime(it) > tstamp for it in files)

        rhash = rstate.get('hash')
        if not do and rhash:
            do = dep.rule.get_hash(target, dep) != rhash

    if not do:
        if not direct:  # order only deps
            state[target] = 'unknown'
        else:
            state[target] = 'uptodate'
        return

    if dep.rule:
        if not tstate:
            dname = os.path.dirname(target)
            dname and makedirs(dname)

        rstate['ts'] = time()
        rstate['hash'] = dep.rule.get_hash(target, dep)
        dep.rule.execute(target, dep)

    state[target] = 'new'
    return rstate


def process_targets(build_targets, tstate=None, always_make=False):
    changed = False
    state = {}
    tstate = tstate or {}
    while True:
        targets = list(iter_stale_leaves(build_targets, {}, state))
        if not targets:
            break
        for t in targets:
            try:
                result = process_target(t, tstate.get(t), state, always_make=always_make)
            except subprocess.CalledProcessError:
                return state, tstate, changed

            if result:
                tstate[t] = result
                changed = True

    return state, tstate, changed


def main():  # pragma: no cover
    import sys
    import runpy
    import argparse
    import json

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-f', dest='rules', metavar='file',
                        default='rules.py', help='File with rules')
    parser.add_argument('-B', '--always-make', action='store_true')
    parser.add_argument('target', nargs='*')

    args = parser.parse_args()

    sys.modules['build'] = sys.modules['__main__']

    rules_file = os.path.abspath(args.rules)
    os.chdir(os.path.dirname(rules_file))
    runpy.run_path(rules_file)

    state_file = os.path.join(os.path.dirname(rules_file), '.build-state')
    if os.path.exists(state_file):
        tstate = json.load(open(state_file))
    else:
        tstate = {}

    if args.always_make:
        tstate = {}

    _, tstate, changed = process_targets(args.target or ['all'], tstate, args.always_make)
    if changed:
        with open(state_file + '.tmp', 'w') as f:
            json.dump(tstate, f)
        os.rename(state_file + '.tmp', state_file)


if __name__ == '__main__':  # pragma: no cover
    main()
