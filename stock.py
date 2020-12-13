import requests
import re

base_url = 'http://hq.sinajs.cn/'


def get_quote(symbol):
    sym = 'gb_' + symbol.lower()
    params = {
        'list': sym
    }
    res = requests.get(url='http://hq.sinajs.cn/', params=params)
    line = res.text
    print('Data received: ' + symbol)
    price = re.findall(",(.*?),", line)[0]
    return price


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
        quotes.append({'symbol': symbol, 'price': float(price)})
    return quotes


if __name__ == '__main__':
    symbol = 'GOOG'
    print(symbol, get_quote(symbol))
    # print(get_quotes(['GOOG', 'AAPL']))
