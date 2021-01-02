import os
import sys
from jinja2 import Environment, FileSystemLoader

env = Environment(loader=FileSystemLoader('.'), trim_blocks=True, lstrip_blocks=True)

t = env.get_template(sys.argv[1])

tmp = sys.argv[2] + '.tmp'
with open(tmp, 'w') as f:
    print(t.render(), file=f)
os.rename(tmp, sys.argv[2])
