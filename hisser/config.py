import os
from urllib.parse import urlsplit

from . import defaults, db, buffer as hbuffer, agg
from .utils import cached_property

TIME_SUFFIXES = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400,
                 'w': 86400 * 7, 'y': 86400 * 365}


def get_config(args):
    result = Config((k, v) for k, v in vars(defaults).items()
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


class Config(dict):
    __getattr__ = dict.__getitem__

    def int(self, name):
        return int(self[name])

    def float(self, name):
        return float(self[name])

    def str(self, name):
        param = self[name]
        if not param:
            raise ValueError('Required param')
        return param

    def host_port(self, name, host='0.0.0.0', port=2003, required=True):
        param = self[name] or ''
        if not param and not required:
            return None
        if param.startswith(':'):
            param = host + param
        url = urlsplit('tcp://' + param)
        return url.hostname, url.port or port

    @cached_property
    def data_dir(self):
        return self.str('DATA_DIR')

    @cached_property
    def retentions(self):
        return parse_retentions(self['RETENTIONS'])

    @cached_property
    def agg_rules(self):
        default = self.str('AGG_DEFAULT_METHOD')
        return agg.AggRules(get_agg_rules_from_dict(self), default)

    @cached_property
    def storage(self):
        return db.Storage(data_dir=self.data_dir,
                          retentions=self.retentions,
                          merge_finder=self.merge_finder,
                          downsample_finder=self.downsample_finder,
                          agg_rules=self.agg_rules)

    @cached_property
    def block_list(self):
        return db.BlockList(data_dir=self.data_dir)

    @cached_property
    def merge_finder(self):
        max_size = self.int('MERGE_MAX_SIZE')
        max_gap_size = self.int('MERGE_MAX_GAP_SIZE')
        ratio = self.float('MERGE_RATIO')

        def finder(resolution, blocks):
            return db.find_blocks_to_merge(resolution, blocks, max_size=max_size,
                                           max_gap_size=max_gap_size, ratio=ratio)
        return finder

    @cached_property
    def downsample_finder(self):
        max_size = self.int('DOWNSAMPLE_MAX_SIZE')
        min_size = self.int('DOWNSAMPLE_MIN_SIZE')
        max_gap_size = self.int('MERGE_MAX_GAP_SIZE')

        def finder(resolution, blocks, new_resolution, start=0):
            return db.find_blocks_to_downsample(
                resolution, blocks, new_resolution,
                max_size=max_size, min_size=min_size,
                max_gap_size=max_gap_size, start=start
            )

        return finder

    @cached_property
    def buffer(self):
        min_res = self.retentions[0][0]
        return hbuffer.Buffer(size=self.int('BUFFER_SIZE'),
                              resolution=min_res,
                              flush_size=self.int('BUFFER_FLUSH_SIZE'),
                              past_size=self.int('BUFFER_PAST_SIZE'),
                              max_points=self.int('BUFFER_MAX_POINTS'))

    @cached_property
    def reader(self):
        return db.Reader(self.block_list, self.retentions)


def parse_retentions(string):
    result = (part.split(':') for part in string.split(','))
    return sorted((parse_seconds(res), parse_seconds(ret)) for res, ret in result)


def parse_aggregation(string):
    return string.rsplit('|', 1)


def parse_seconds(interval):
    if isinstance(interval, int):
        return interval

    interval = interval.strip()
    if interval.isdigit():
        return int(interval)

    return int(interval[:-1]) * TIME_SUFFIXES[interval[-1]]


def get_agg_rules_from_dict(cfg):
    rules = sorted((k, v)
                   for k, v in cfg.items()
                   if k.startswith('AGG_RULE_') and v)

    return [parse_aggregation(v) for _, v in rules]
