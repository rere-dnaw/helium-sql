from models import FearGreed, Prices, Coins, BurnedDC, InflationHNT
from base import Session, engine, Base
import statics
import ccxt
from datetime import datetime
import my_methods
import requests
from requests.adapters import HTTPAdapter, Retry
import db_insert_DC
import db_insert_inflationHNT



exchange = ccxt.binance({
    'apiKey': statics.BINANCE_API_KEY,
    'secret': statics.BINANCE_SECRET_KEY
})


Base.metadata.create_all(engine)

session = Session()


retry_strategy = Retry(
    total=20,
    status_forcelist=[429, 500, 502, 503, 504],
    method_whitelist=["HEAD", "GET", "OPTIONS"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("https://", adapter)
http.mount("http://", adapter)


def add_price(row, pairID, interval):
    '''
    This method will add token info into db.
    '''
    row.insert(1, datetime.utcfromtimestamp(int(row[0]/1000)).strftime('%Y-%m-%d %H:%M:%S'))
    row.insert(2, interval)
    price = Prices(coin_id = pairID,
                    time_stamp = row[0],
                    date = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S"),
                    interval = row[2],
                    open = row[3],
                    high = row[4],
                    low = row[5],
                    close = row[6],
                    volume = row[7])
                
    session.add(price)


def add_coin(coin):
    '''
    This method will add token pait into coin table.
    '''
    pairs = [item[0] for item in session.query(Coins).with_entities(Coins.name).all()]
    if coin not in pairs:
        session.add(Coins(name = coin))


def get_coin_id(pair):
    '''
    The function will return
    id for provided token pair.
    '''
    pair_id = session.query(Coins).with_entities(Coins.id) \
                .filter(Coins.name.like(pair)).all()
    return [item[0] for item in pair_id][0]


def add_feergreed(row):
    '''
    Will add fear and greed index balue into FearGreed table.
    '''
    row['date'] = datetime.fromtimestamp(int(row['timestamp'])).strftime('%Y-%m-%d %H:%M:%S')
    indexFG = FearGreed(time_stamp = int(row['timestamp']),
                        date = datetime.strptime(row['date'], "%Y-%m-%d %H:%M:%S"),
                        value = int(row['value']),
                        classification= row['value_classification'])
    session.add(indexFG)


def init_db():
    '''
    This method will fill db table 
    '''

    hours = int(my_methods.count_hours(datetime.strptime(statics.START_DAY, "%Y-%m-%d %H:%M:%S"),
                                datetime.now()))

    days = int(my_methods.count_days(datetime.strptime(statics.START_DAY, "%Y-%m-%d %H:%M:%S"),
                                datetime.now()))
 

    fill_db(days, hours)


def fill_db(days, hours):
    '''
    '''
    for pair in statics.TOKEN_LIST:
        add_coin(pair)
        session.commit()

        insert_1h_interval(pair, hours)
        
        insert_4h_interval(pair, int(round(hours/4,0)))

        insert_1d_interval(pair, days)

    fear_gree_index = http.get('https://api.alternative.me/fng/?limit={0}'.format(days)).json()
    for fear_greed_val in fear_gree_index['data']:
        add_feergreed(fear_greed_val)


    db_insert_DC.pull_data_DC_burned()
    db_insert_inflationHNT.pull_HNT_inflation_data()

    session.commit()


def insert_1h_interval(pair, hours):
    '''
    '''
    pairID = get_coin_id(pair)
    interval = '1h'
    data = exchange.fetch_ohlcv(pair, interval, limit=hours)
    for row in data:
        add_price(row, pairID, interval)
    session.commit()


def insert_4h_interval(pair, hours):
    '''
    '''
    pairID = get_coin_id(pair)
    interval = '4h'
    data = exchange.fetch_ohlcv(pair, interval, limit=hours)
    for row in data:
        add_price(row, pairID, interval)
    session.commit()


def insert_1d_interval(pair, days):
    '''
    '''
    pairID = get_coin_id(pair)
    interval = '1d'
    data = exchange.fetch_ohlcv(pair, interval, limit=days)
    for row in data:
        add_price(row, pairID, interval)
    session.commit()


if session.query(Coins).first() is None:
    init_db()


session.commit()
session.close()