import requests
import json

base_url = 'https://www.alphavantage.co/query?'
params = {
    'function': 'TIME_SERIES_DAILY',
    'symbol': 'IBM',
    'outputsize': 'compact',
    'apikey': 'Y2617R1QN0XGOBFN'
}

res = requests.get(url=base_url, params=params).json()
symbol = res['Meta Data']['2. Symbol']
quotes = res['Time Series (Daily)']
print(symbol)
print(quotes)
