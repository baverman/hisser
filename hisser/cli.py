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
        print(k.decode(), len(v), v, sep='\t')


@cli.command('dump-index', help='dump content of metric index')
@click.option('-t', '--type', 'itype', default='tag-names',
              type=click.Choice(['tree', 'tags', 'tag-names']))
@click.argument('index')
def cmd_dump_index(itype, index):
    mi = metrics.MetricIndex(index)
    if itype == 'tree':
        for k, v in mi.iter_tree():
            print(k.decode(), v.decode(), sep='\t')
    elif itype == 'tags':
        for k, v in mi.iter_tags():
            print(k.decode(), v.decode(), sep='\t')
    elif itype == 'tag-names':
        for k, v in mi.iter_tag_names():
            print(k.decode(), v.decode(), sep='\t')


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
