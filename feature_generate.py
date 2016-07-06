import numpy as np
import datetime


def into_season(date_time):
    if isinstance(date_time, str):
        month = int(date_time.split('-')[1])
    else:
        try:
            month = date_time.month
        except:
            print 'not support transformation with value %s, type %s' % date_time % type(date_time)
            return np.nan
    if month == 3 or month == 4 or month == 5:
        return 'Spring'
    elif month == 6 or month == 7 or month == 8:
        return 'Summer'
    elif month == 9 or month == 10 or month == 11:
        return 'Fall'
    elif month == 12 or month == 1 or month == 2:
        return 'Winter'
    else:
        return np.nan


def into_is_weekend(date_time):
    if isinstance(date_time,str):
        if ':' in date_time:
            date_time = datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S')
        else:
            date_time = datetime.strptime(date_time, '%Y-%m-%d')
        return date_time.weekday() > 5
    elif np.isnan(date_time):
        return np.nan
    else:
        raise Exception('No support for this date type')