# Default parameters for hisser
#
# All time values can have interval suffixes: (s)econds, (m)inutes,
# (h)ours, (d)ays, (w)eeks and (y)ears. Seconds by default.

# Directory with data, can be set via `hisser` `--data-dir` option.
DATA_DIR = None

# Default retentions, resolution:keep-interval,...
# Greater resolution must be an integer factor of a smaller one.
RETENTIONS = '1m:3d, 5m:15d, 30m:90d, 2h:1y'

# Default aggregations, pattern|method,...
# Patterns are applied in sorted order.
# You can test it with `hisser agg-method <metric name>` command.
AGG_RULE_MAX = r'\.max$|max'
AGG_RULE_MIN = r'\.min$|min'
AGG_RULE_SUM = r'\.count$|sum'
# AGG_RULE_CUSTOM_RULE = r'my_pattern|avg'

# Default aggregation if no any rules matched
AGG_DEFAULT_METHOD = 'avg'

# How much points of each metric to keep in memory.
# With default minimal resolution of 60s it will be 30 minutes of data.
BUFFER_SIZE = 30

# How much data can be accepted from the past. 5 * 60s is 5 minutes.
BUFFER_PAST_SIZE = 5

# Flush buffer on reaching this size. Buffer will be flushed every 10 minutes
# with default flush size and 60s resolution.
BUFFER_FLUSH_SIZE = 10

# Flush buffer on reaching points limit. Points are equal to number of metrics in
# buffer multiplied by current buffer position. For example if your instance
# receive 100k metrics per minute, then it will be flushed every 5 minutes for default
# resolution of 60s.
BUFFER_MAX_POINTS = 500000

BUFFER_COMPACT_RATIO = 0.9

MERGE_MAX_SIZE = 700
MERGE_MAX_GAP_SIZE = 30
MERGE_RATIO = 1.4

DOWNSAMPLE_MIN_SIZE = 10
DOWNSAMPLE_MAX_SIZE = 1000

# Listen tcp `[host]:port` for carbon text protocol,
# by default host is 0.0.0.0.
CARBON_BIND = ':2003'

# Listen udp `[host]:port` for carbon text protocol
CARBON_BIND_UDP = None

# Listen backlog for carbon protocol
CARBON_BACKLOG = 100

# Listen tcp `[host]:port` for link protocol,
LINK_BIND = '127.0.0.1:8002'

# Listen backlog for link protocol
LINK_BACKLOG = 100

# Python logging dict, if none log to stdout
LOGGING = None

# Default log level
LOGGING_LEVEL = 'ERROR'
