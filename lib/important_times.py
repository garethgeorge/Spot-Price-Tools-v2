from lib.time_util import time_to_epoch
from datetime import datetime
BEFORE_END=datetime.strptime("2017-11-01 00:00:00", "%Y-%m-%d %H:%M:%S")
BEFORE_END_EPOCH=time_to_epoch(BEFORE_END)
AFTER_START=datetime.strptime("2017-12-01 00:00:00", "%Y-%m-%d %H:%M:%S")
AFTER_START_EPOCH=time_to_epoch(AFTER_START)