from functools import wraps
import click
from . import config, db, server, defaults, agg


@click.group()
def cli():
    pass


def config_aware(func):
    @wraps(func)
    def inner(**kwargs):
        cfg = config.get_config(kwargs)
        return func(cfg)
    return inner


def common_options(func):
    func = click.option('--config', '-c', '_config', metavar='path', help='path to config file')(func)
    func = click.option('--data-dir', '-d', metavar='path', help='path to directory with data')(func)
    return func


@cli.command('merge', help='merge two or more blocks')
@common_options
@click.argument('block1')
@click.argument('block2')
@click.argument('block', nargs=-1)
@config_aware
def cmd_merge(cfg):
    db.merge(cfg.data_dir, [cfg.BLOCK1, cfg.BLOCK2] + list(cfg.BLOCK))


@cli.command('downsample', help='run downsample')
@common_options
@config_aware
def cmd_downsample(cfg):
    cfg.storage.do_downsample()


@cli.command('dump', help='dump content of block')
@click.argument('block')
@config_aware
def cmd_dump(cfg):
    for k, v in db.dump(cfg.BLOCK):
        print(k.decode(), len(v), v)


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
              help='host and port to listen carbon text protocol, default is {}'.format(defaults.CARBON_BIND))
@config_aware
def cmd_run(cfg):
    server.loop(buf=cfg.buffer,
                storage=cfg.storage,
                host_port=cfg.host_port('CARBON_BIND'),
                backlog=cfg.int('CARBON_BACKLOG'))


@cli.command('agg-method', help='show aggregation method for metric')
@click.argument('metric', metavar='metric.name')
@config_aware
def cmd_agg_method(cfg):
    method = cfg.agg_rules.get_method(cfg.METRIC)
    rmethods = {v:k for k, v in agg.METHODS.items()}
    print(rmethods[method])


@cli.command('test')
@common_options
@config_aware
def cmd_test(cfg):
    print(cfg.reader.fetch(['localhost.cpu.percent.idle'], 1515435224, 1516040024))


if __name__ == '__main__':
    cli(prog_name='hisser')
