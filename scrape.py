import requests as r
import pandas as pd
from time import sleep
from bs4 import BeautifulSoup
from functools import wraps
from pdb import set_trace
import re 
from sys import argv

TIME_OUT = 1

# SCHX
# http://etfdb.com/data_set/?tm=1715&cond={%22by_etf%22:910}&no_null_sort=true&count_by_id=&sort=weight&order=desc&limit=15&offset=30

# SPY
# http://etfdb.com/data_set/?tm=1715&cond={%22by_etf%22:325}&no_null_sort=true&count_by_id=&sort=weight&order=desc&limit=15&offset=75

class Data():

    def __init__(self, name, ticker_number, total):
        self.name = name
        self.ticker_number = ticker_number
        self.total = total

def zzz(func):
    @wraps(func)
    def with_zzz(*args, **kwargs):
        print ("Sleeping for {}".format(TIME_OUT))
        sleep(TIME_OUT)
        return func(*args, **kwargs)
    return with_zzz

@zzz
def make_request(ticker):
    url = "http://etfdb.com/etf/{}/".format(ticker.upper())
    html = r.get(url).text
    soup = BeautifulSoup(html, 'lxml')
    return soup

def get_etfcode(soup):
    data = soup.select_one('#etf-holdings').get("data-url")
    pattern = re.compile(r"by_etf\":(\d+)")
    number = pattern.findall(data).pop()
    return number

def total(soup):
    address = "#holdings-collapse > div > div > div.col-md-6.divider-vertical > h3"
    data = soup.select_one(address)
    pattern = re.compile(r"All (\d+)")
    return pattern.findall(data.text).pop()

def get_json_data(id, limit):

    base_url = "http://etfdb.com/data_set/?tm=1715&cond={{%22by_etf%22:{id}}}&no_null_sort=true&count_by_id=&sort=weight&order=desc&limit={limit}&offset=0"
    url = base_url.format(id = id, limit = limit, )
    res = r.get(url)
    json = res.json()
    rows = json['rows']

    def process (x):
        weight = x['weight']
        holding = x['holding']
        def get_stock(x):

            pattern = re.compile(r"/stock/([\w.:]+)/")
            return pattern.findall(x).pop()


        try:
            if holding.startswith("Cash"):
                stock = "Cash"
            else:
                stock = get_stock(holding)
        except Exception as e:
            foo = e

        return (stock, weight.strip("%"))

    return [process(x) for x in rows]

def to_df(ticker,list_of_tuples):
    df = pd.DataFrame(list_of_tuples, columns = ['Holding','Weight-(%)'])
    return df

def main(ticker):
    soup = make_request(ticker)
    number = get_etfcode(soup)
    total_records = total(soup)
    res = get_json_data(number, total_records)
    df = to_df(ticker, res)
    df.to_csv('data/{}.csv'.format(ticker), index = False)

if __name__ == "__main__":
    ticker = argv[1]
    main(ticker)