import os
import sys
from functools import wraps

import click
from . import config, db, defaults, agg, metrics


@click.group()
def cli():
    pass


def config_aware(func):
    @wraps(func)
    def inner(**kwargs):
        cfg = config.get_config(kwargs)
        cfg.setup_logging(func.__name__ == 'cmd_run')
        try:
            return func(cfg)
        except config.Config.Error as e:
            print(str(e), file=sys.stderr)
            sys.exit(1)

    return inner


def common_options(func):
    func = click.option('--config', '-c', '_config', metavar='path', help='path to config file')(func)
    func = click.option('--data-dir', '-d', metavar='path', help='path to directory with data')(func)
    return func


@cli.command('merge', help='merge two or more blocks')
@common_options
@click.option('-r', 'resolution', type=int)
@click.argument('block1')
@click.argument('block2')
@click.argument('block', nargs=-1)
@config_aware
def cmd_merge(cfg):
    db.merge(cfg.data_dir, cfg.RESOLUTION, [cfg.BLOCK1, cfg.BLOCK2] + list(cfg.BLOCK))


@cli.command('downsample', help='run downsample')
@common_options
@config_aware
def cmd_downsample(cfg):
    cfg.storage.do_downsample()


@cli.command('cleanup', help='remove old blocks')
@common_options
@config_aware
def cmd_cleanup(cfg):
    cfg.storage.do_cleanup()


@cli.command('dump', help='dump content of block')
@click.argument('block')
@config_aware
def cmd_dump(cfg):
    for k, v in db.dump(cfg.BLOCK):
        print(k, len(v), v)


@cli.command('dump-index', help='dump content metric index')
@click.argument('index')
@config_aware
def cmd_dump_index(cfg):
    mi = metrics.MetricIndex(cfg.INDEX)
    for k, v in mi.iter_tree():
        print(k.decode(), v.decode())


@cli.command('check', help='checks metadata')
@click.argument('block', nargs=-1)
@config_aware
def cmd_check(cfg):
    for path in cfg.BLOCK:
        block = db.get_info(path)
        result = set()
        for _k, v in db.dump(path):
            result.add(len(v))

        if len(result) > 2 or list(result)[0] != block.size:
            print(path, 'Invalid sizes', sorted(result))


@cli.command('run', help='run server')
@common_options
@click.option('--carbon-bind', '-l', metavar='[host]:port',
              help='host and port to listen carbon text protocol on tcp, default is {}'.format(defaults.CARBON_BIND))
@click.option('--carbon-bind-udp', metavar='[host]:port',
              help='host and port to listen carbon text protocol on udp, default is {}'.format(defaults.CARBON_BIND_UDP))
@config_aware
def cmd_run(cfg):
    for r, _ in cfg.retentions:
        os.makedirs(os.path.join(cfg.data_dir, str(r)), exist_ok=True)

    server = cfg.server
    server.listen()
    server.run()


@cli.command('agg-method', help='show aggregation method for metric')
@click.argument('metric', metavar='metric.name')
@config_aware
def cmd_agg_method(cfg):
    method = cfg.agg_rules.get_method(cfg.METRIC)
    rmethods = {v: k for k, v in agg.METHODS.items()}
    print(rmethods[method])


# @cli.command('test')
# @common_options
# @config_aware
# def cmd_test(cfg):
#     # print(cfg.reader.fetch(['localhost.cpu.percent.idle'], 1515435224, 1516040024))
#     print(cfg.rpc_client.call('fetch', keys=[b'random.diceroll', b'localhost.cpu.percent.idle', 'foo']))


if __name__ == '__main__':
    cli(prog_name='hisser')
