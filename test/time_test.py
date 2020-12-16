from datetime import datetime
import datetime as dt

def convert_hour(hour):
    return (hour+24-13) % 24


time = datetime.now().replace(second=0, microsecond=0)

print(str(time))

