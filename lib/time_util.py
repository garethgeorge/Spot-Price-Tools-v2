from datetime import datetime , timezone
import calendar, datetime, time

def time_to_epoch(timestamp):
    epoch = calendar.timegm(timestamp.utctimetuple())
    return epoch
