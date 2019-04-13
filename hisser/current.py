import os
from hisser.config import get_config


config = get_config({}, os.environ.get('HISSER_CONFIG'))
