import requests
import re

base_url = 'http://hq.sinajs.cn/'

map = {'Dec': 12}

def get_quote(symbol):
    sym = 'gb_' + symbol.lower()
    params = {
        'list': sym
    }
    res = requests.get(url=base_url, params=params)
    line = res.text
    # print('Data received: ' + symbol)
    strs = line.split(',')
    # print(strs)
    # print(strs[-10])
    price = strs[1]
    time = get_time(strs[-10])
    return symbol, price, time


def get_time(str):
    print(str)
    # Dec 11 04:00PM EST
    month, day, clock, zone = str.split(' ')
    month = map[month]
    time = clock[:5]
    noon = clock[-2:]
    h, m = time.split(':')
    h = int(h)
    if noon == 'PM':
        h += 12
    return '%s-%s-%s %s:%s:00' % (2020, month, day, h, m)


def get_quotes(symbols):
    syms = []
    for symbol in symbols:
        syms.append('gb_' + symbol.lower())
    params = 'list=' + ','.join(syms)
    res = requests.get(base_url + params)
    data = res.text
    lines = data.split('\n')
    quotes = []
    for i in range(len(symbols)):
        symbol = symbols[i]
        line = lines[i]
        price = re.findall(",(.*?),", line)[0]
        quotes.append(symbol + '\t' + price)
    return quotes


if __name__ == '__main__':
    symbol = 'AAPL'

    print(get_quote(symbol))
    # print(get_time('04:00PM'))
    # print(get_quotes(['GOOG', 'AAPL']))
