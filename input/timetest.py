#!/usr/bin/env python
# coding=utf-8
'''
Author: Jin X
Date: 2020-12-15 15:41:19
LastEditTime: 2020-12-15 17:11:32
'''
from dateutil.parser import parse
from datetime import timedelta

def floorAndCeil(dt, interval="s"):
    if(dt.isoweekday() != 1):
        floorTime = timedelta(days=1, seconds=dt.second, microseconds=dt.microsecond, milliseconds=0, minutes=dt.minute, hours=dt.hour, weeks=0)
    else:
        floorTime = timedelta(days=3, seconds=dt.second, microseconds=dt.microsecond, milliseconds=0, minutes=dt.minute, hours=dt.hour, weeks=0)
    ceilTime = timedelta(days=0, seconds=dt.second, microseconds=dt.microsecond, milliseconds=0, minutes=dt.minute, hours=dt.hour, weeks=0)
    # if (dt.isoweekday() != 5):
    #     ceilTime = timedelta(days=0, seconds=dt.second, microseconds=dt.microsecond, milliseconds=0, minutes=dt.minute, hours=dt.hour, weeks=0)
    # else:
    #     ceilTime = timedelta(days=-3, seconds=dt.second, microseconds=dt.microsecond, milliseconds=0, minutes=dt.minute, hours=dt.hour, weeks=0)
    newFloorTime = dt - floorTime
    newCeilTime = dt - ceilTime
    
    print(dt.isoweekday(), newFloorTime.isoweekday(), newCeilTime.isoweekday())
    return str(newCeilTime), str(newFloorTime)


print(parse("2020-12-11").timestamp())

print(floorAndCeil(parse("2020-12-7")))