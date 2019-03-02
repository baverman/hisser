import os
import sys
from functools import wraps

import click
from . import config, db, defaults, agg, metrics, utils, version


@click.group()
@click.version_option(version=version)
def cli():
    pass


def config_aware(func):
    @click.option('--config', '-c', 'config_path',
                  metavar='path', help='path to config file')
    @click.option('--data-dir', '-d', metavar='path',
                  help='path to directory with data')
    @click.pass_context
    @wraps(func)
    def inner(ctx, config_path, **kwargs):
        config_path = config_path or os.environ.get('HISSER_CONFIG')
        cfg = config.get_config(kwargs, config_path)
        cfg.setup_logging(func.__name__ == 'cmd_run')
        try:
            return ctx.invoke(func, cfg, **kwargs)
        except config.Config.Error as e:
            print(str(e), file=sys.stderr)
            sys.exit(1)

    return inner


@cli.command('merge', help='merge two or more blocks')
@click.option('-r', 'resolution', metavar='seconds', type=int,
              help='targer resolution')
@click.argument('blocks', metavar='[block]...', nargs=-1)
@config_aware
def cmd_merge(cfg, resolution, blocks):
    if blocks:
        db.merge(cfg.data_dir, resolution, list(blocks))
    else:
        cfg.storage.do_merge()


@cli.command('downsample', help='run downsample')
@config_aware
def cmd_downsample(cfg):
    cfg.storage.do_downsample()


@cli.command('cleanup', help='remove old blocks')
@config_aware
def cmd_cleanup(cfg):
    cfg.storage.do_cleanup()


@cli.command('dump', help='dump content of block')
@click.argument('block')
def cmd_dump(block):
    for k, v in db.dump(block):
        print(k, len(v), v, sep='\t')


@cli.command('dump-index', help='dump content of metric index')
@click.option('-t', '--type', 'itype', default='names',
              type=click.Choice(['names', 'tags']))
@click.argument('index')
def cmd_dump_index(itype, index):
    mi = metrics.MetricIndex(index)
    if itype == 'tags':
        for k, v in mi.iter_tags():
            print(k.decode(), v.decode(), sep='\t')
    elif itype == 'names':
        for v in mi.iter_names():
            print(v.decode())


@cli.command('reindex', help='Reindex metrics')
@click.argument('index')
@click.argument('name_block', nargs=-1, required=True)
def cmd_reindex(index, name_block):
    mi = metrics.MetricIndex(index)
    if name_block[0] == '-':
        def g():
            for line in sys.stdin.buffer:
                yield line.rstrip(b'\n')
        mi.add(g())
    else:
        for b in name_block:
            mi.add(db.read_name_block(b))


@cli.command('dump-name-block', help='dump contents of .hdbm file(s)')
@click.argument('name_block', nargs=-1, required=True)
@click.option('-v', 'verbose', is_flag=True)
@config_aware
def cmd_dump_name_block(cfg, name_block, verbose):
    if verbose:
        rmethods = {v: k for k, v in agg.METHODS.items()}
        for block in name_block:
            for line in db.read_name_block(block):
                method = cfg.agg_rules.get_method(line, use_bin=True)
                print(line.decode(), utils.make_key(line), rmethods[method], sep='\t')
    else:
        for block in name_block:
            db.dump_name_block(block, sys.stdout.buffer)


@cli.command('backup', help='backup db file')
@click.argument('dbfile')
@click.argument('out')
def cmd_backup(dbfile, out):
    with open(out, 'wb') as f:
        with utils.open_env(dbfile) as env:
            env.copyfd(f.fileno(), True)


@cli.command('check', help='checks metadata')
@click.argument('blocks', metavar='[block]...', nargs=-1)
def cmd_check(blocks):
    for path in blocks:
        block = db.get_info(path)
        result = set()
        for _k, v in db.dump(path):
            result.add(len(v))

        if len(result) > 2 or list(result)[0] != block.size:
            print(path, 'Invalid sizes', sorted(result))


@cli.command('run', help='run server')
@click.option('--carbon-bind', '-l', metavar='[host]:port',
              help=('host and port to listen carbon text'
                    ' protocol on tcp, default is {}').format(defaults.CARBON_BIND))
@click.option('--carbon-bind-udp', metavar='[host]:port',
              help=('host and port to listen carbon'
                    ' text protocol on udp, default is {}').format(defaults.CARBON_BIND_UDP))
@click.option('--link-bind', metavar='[host]:port',
              help=('host and port to listen graphite finder link protocol'
                    ', default is {}').format(defaults.LINK_BIND))
@config_aware
def cmd_run(cfg):
    cfg.ensure_dirs()
    server = cfg.server
    server.listen()
    server.run()


@cli.command('agg-method', help='show aggregation method for metric')
@click.argument('names', metavar='[name]...', nargs=-1)
@config_aware
def cmd_agg_method(cfg, names):
    rmethods = {v: k for k, v in agg.METHODS.items()}
    for n in names:
        method = cfg.agg_rules.get_method(n)
        print(n, rmethods[method], sep='\t')


if __name__ == '__main__':
    cli(prog_name='hisser')
