from datetime import datetime
import datetime as dt

def convert_hour(hour):
    return (hour+24-13) % 24


delta = dt.timedelta(hours=13)
now = datetime.today() - delta
print(now)

