from prometheus_client import start_http_server, Gauge
import time
import requests as rq
import pandas as pd

API_BINANCE_URL = "https://api.binance.com/api"

class BinanceAPI():
    def __init__(self, API_BINANCE_URL):
        self.API_BINANCE_URL = API_BINANCE_URL
        self.gauge_abs_delta_value = Gauge('symbols_absolute_delta_value',
                                           'Symbols absolute delta value',
                                           ['symbol'])

    def getTopSymbols(self, symbol_asset, field, print_output=False):
        uri = "/v3/ticker/24hr"
        r = rq.get(self.API_BINANCE_URL + uri)
        df = pd.DataFrame(r.json())
        df = df[['symbol', field]]
        df = df[df.symbol.str.contains(r'(?!$){}$'.format(symbol_asset))]
        df[field] = pd.to_numeric(df[field],
                                  downcast='float',
                                  errors='coerce')
        df = df.sort_values(ascending=False, by=[field]).head(5)
        if print_output:
            print("Get top symbol asset for %s by %s" % (symbol_asset, field))
            print(df)
        return df

    def getNotional(self, symbol_asset, field, print_output=False):
        notional_list = {}
        uri = "/v3/depth"
        symbols = self.getTopSymbols(symbol_asset, field)

        for sym in symbols['symbol']:
            data_load = {
                'symbol' : sym,
                'limit' : 500
            }
            r = rq.get(self.API_BINANCE_URL + uri, params=data_load)
            for col in ["bids", "asks"]:
                df = pd.DataFrame(data=r.json()[col],
                                  columns=["price", "quantity"],
                                  dtype=float)
                df = df.sort_values(by=['price'],
                                    ascending=False).head(200)
                df['notional'] = df['price'] * df['quantity']
                df['notional'].sum()
                notional_list[sym + '_' + col] = df['notional'].sum()

        if print_output:
            print("Get total notional value of %s by %s" %  (symbol_asset, field))
            print(notional_list)

        return notional_list

    def getPriceSpread(self, symbol_asset, field, print_output=False):
        spread_list = {}
        uri = '/v3/ticker/bookTicker'
        symbols = self.getTopSymbols(symbol_asset, field)

        for s in symbols['symbol']:
            data_load = {
                'symbol' : s
            }
            r = rq.get(self.API_BINANCE_URL + uri,
                       params=data_load)
            price_spread = r.json()
            spread_list[s] = float(price_spread['askPrice']) - float(price_spread['bidPrice'])

        if print_output:
            print("Get price spread for %s by %s" % (symbol_asset, field))
            print(spread_list)

        return spread_list

    def getSpreadAbsolute(self, symbol_asset, field):
        while True:
            delta_list = {}
            spread_old = self.getPriceSpread(symbol_asset, field)
            time.sleep(10)
            spread_new = self.getPriceSpread(symbol_asset, field)

            for key in spread_old:
                delta_list[key] = abs(spread_old[key]-spread_new[key])

            for key in delta_list:
                self.gauge_abs_delta_value.labels(key).set(delta_list[key])

            print("Get value absolute delta of %s" % symbol_asset)
            print(delta_list)

    def checkServiceStatus(self):
        r = rq.get(self.API_BINANCE_URL + "/v3/ping")
        if (r.status_code != 200):
            raise Exception('Binance API service is not working!')

if __name__ == "__main__":
    service = BinanceAPI(API_BINANCE_URL)
    service.checkServiceStatus()
    start_http_server(8080)

    print ("1. Print the top 5 symbols with quote asset BTC and the highest volume over the last 24 hours in descending order.:")
    service.getTopSymbols('BTC','volume',True)
    print ("======================================")

    print ("2. Print the top 5 symbols with quote asset USDT and the highest number of trades over the last 24 hours in descending order.")
    service.getTopSymbols('USDT', 'count', True)
    print ("======================================")

    print ("3. Using the symbols from Q1, what is the total notional value of the top 200 bids and asks currently on each order book?")
    service.getNotional('BTC', 'volume', True)
    print ("======================================")

    print ("4. What is the price spread for each of the symbols from Q2?")
    service.getPriceSpread('USDT', 'count', True)
    print ("======================================")

    print ("5. Every 10 seconds print the result of Q4 and the absolute delta from the previous value for each symbol.")
    service.getSpreadAbsolute('USDT', 'count')
    print ("======================================")

    print ("6. Make the output of Q5 accessible by querying http://localhost:8080/metrics using the Prometheus Metrics format.")
    print ("Please access http://localhost:8080/metrics or http://127.0.0.1:8080/metrics to view results")
