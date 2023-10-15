# Default parameters for hisser
#
# All time values can have interval suffixes: (s)econds, (m)inutes,
# (h)ours, (d)ays, (w)eeks and (y)ears. Seconds by default.

# Directory with data, can be set via `hisser` `--data-dir` option.
DATA_DIR = None

# Default retentions, resolution:keep-interval,...
# Greater resolution must be an integer factor of a smaller one.
RETENTIONS = '1m:3d, 5m:15d, 30m:90d, 2h:1y'

# This configuration setting is for testing purposes only
# If True disables automatic merge, downsample and data cleanup.
DISABLE_HOUSEWORK = False

# Default aggregations, pattern|method,...
# Patterns are applied in sorted order.
# You can test it with `hisser agg-method <metric name>` command.
AGG_RULE_MAX = r'\.(max|upper)($|;)|max'
AGG_RULE_MIN = r'\.(min|lower)($|;)|min'
AGG_RULE_SUM = r'\.(count|sum)($|;)|sum'
# AGG_RULE_CUSTOM_RULE = r'my_pattern|avg'

# Default aggregation if no any rules matched
AGG_DEFAULT_METHOD = 'avg'

# Number of points to flush. Buffer will be flushed every 10 minutes
# with default flush size and 60s resolution.
BUFFER_FLUSH_SIZE = 10

BUFFER_FUTURE_TOLERANCE = 5

# Remove metrics without points from buffer on total_metrics/non_empty_metrics
# ratio threshold.
BUFFER_COMPACT_RATIO = 1.5

# Maximum size of merged block in points.
MERGE_MAX_SIZE = 700

# Maximum gap between blocks being merged and downsampled in points. If gap
# is greater then each block will be placed in different merged/downsampled block.
MERGE_MAX_GAP_SIZE = 30

# Size ratio between blocks to allow it to be merged. It is needed to
# prohibit repeated merging of a big block with a small one.
MERGE_RATIO = 1.4

# Minimal size of block to downsample in points.
DOWNSAMPLE_MIN_SIZE = 10

# Maximum size of final downsampled block in points.
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

# Slow log threshold
SLOW_LOG = 0.1
