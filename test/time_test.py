from datetime import datetime
import time

def convert_hour(hour):
    return (hour+24-13) % 24

t1 = 'Nov 11 04:00PM EST'
t2 = 'Dec 11 04:00PM EST'
d1 = datetime.strptime('17:50', '%H:%M')
d2 = datetime.strptime('17:41', '%H:%M')

delta = d1 - d2
print(delta.seconds)
now = datetime.now()
hour = now.hour

hour = convert_hour(int(hour))
nowtime = str(hour) + ':' + str(now.minute)
print(nowtime)
