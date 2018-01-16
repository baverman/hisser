import os
from urllib.parse import urlsplit
from functools import wraps

import click

from . import config, db, server, buffer as hbuffer
from .utils import parse_retentions, cached_property


class Config(dict):
    __getattr__ = dict.__getitem__

    def int(self, name):
        return int(self[name])

    def float(self, name):
        return float(self[name])

    def str(self, name, required=False):
        param = self[name]
        if required and not param:
            raise ValueError('Required param')
        return param

    def host_port(self, name, host='0.0.0.0', port=2003):
        param = self[name] or ''
        if param.startswith(':'):
            param = host + param
        url = urlsplit('tcp://' + param)
        return url.hostname, url.port or port

    @cached_property
    def data_dir(self):
        return self.str('DATA_DIR', required=True)

    @cached_property
    def retentions(self):
        return parse_retentions(self['RETENTIONS'])

    @cached_property
    def storage(self):
        return db.Storage(data_dir=self.data_dir,
                          merge_finder=self.merge_finder)

    @cached_property
    def block_list(self):
        return db.BlockList(data_dir=self.data_dir)

    @cached_property
    def merge_finder(self):
        max_size=self.int('MERGE_MAX_SIZE')
        max_gap_size=self.int('MERGE_MAX_GAP_SIZE')
        keep_size=self.int('MERGE_KEEP_SIZE')
        ratio=self.float('MERGE_RATIO')

        def merge_finder(resolution, blocks):
            return db.find_blocks_to_merge(resolution, blocks, max_size=max_size,
                                           keep_size=keep_size, max_gap_size=max_gap_size,
                                           ratio=ratio)
        return merge_finder

    @cached_property
    def buffer(self):
        min_res = self.retentions[0]
        return hbuffer.Buffer(size=self.int('BUFFER_SIZE'),
                              resolution=min_res,
                              flush_size=self.int('BUFFER_FLUSH_SIZE'),
                              past_size=self.int('BUFFER_PAST_SIZE'),
                              max_points=self.int('BUFFER_MAX_POINTS'))

    @cached_property
    def reader(self):
        return db.Reader(self.block_list, self.retentions)


def get_config(args):
    result = Config((k, v) for k, v in vars(config).items()
                    if not k.startswith('_'))

    if args.get('_config'):
        from runpy import run_path
        result.update(run_path(args['_config']))

    for k, v in args.items():
        if not k.startswith('_') and v is not None:
            result[k.upper()] = v

    for k in result:
        if k in os.environ:
            result[k] = os.environ[k]

    return result


@click.group()
def cli():
    pass


def config_aware(func):
    @wraps(func)
    def inner(**kwargs):
        cfg = get_config(kwargs)
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
    blocks = cfg.block_list.blocks(60)
    if not blocks:
        return

    lblocks = cfg.block_list.blocks(300)
    start = (lblocks and lblocks[-1].end) or 0
    segments = db.find_blocks_to_downsample(60, blocks, 300, 30, 10, 700, start)
    db.downsample(cfg.data_dir, 300, segments)


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
              help='host and port to listen carbon text protocol, default is {}'.format(config.CARBON_BIND))
@config_aware
def cmd_run(cfg):
    server.loop(buf=cfg.buffer,
                storage=cfg.storage,
                host_port=cfg.host_port('CARBON_BIND'),
                backlog=cfg.int('CARBON_BACKLOG'))


@cli.command('test')
@common_options
@config_aware
def cmd_test(cfg):
    print(cfg.reader.fetch(['localhost.cpu.percent.idle'], 1515435224, 1516040024))


if __name__ == '__main__':
    cli(prog_name='hisser')
