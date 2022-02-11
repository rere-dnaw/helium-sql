## run every hour

from functools import partial
from models import Prices, Coins
from base import Session, engine, Base
import statics
import ccxt
from datetime import datetime
import my_methods



exchange = ccxt.binance({
    'apiKey': statics.BINANCE_API_KEY,
    'secret': statics.BINANCE_SECRET_KEY
})


Base.metadata.create_all(engine)

session = Session()


def add_price(row, pairID, interval):
    '''
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
    '''
    pairs = [item[0] for item in session.query(Coins).with_entities(Coins.name).all()]
    if coin not in pairs:
        session.add(Coins(name = coin))


def get_coin_id(pair):
    '''
    '''
    pair_id = session.query(Coins).with_entities(Coins.id) \
                .filter(Coins.name.like(pair)).all()
    return [item[0] for item in pair_id][0]


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


def add_missing_price():
    '''
    '''

    pairList = [item[0] for item in session.query(Coins.name).all()]

    for pair in pairList:
        coinID = get_coin_id(pair)

        date_last_1h = session.query(Prices).filter(Prices.coin_id.like(coinID)).filter(Prices.interval.like('1h')).order_by(Prices.date.desc()).first().date
        # hours - 1 because of UTC
        hours = int(my_methods.count_hours(date_last_1h, datetime.now())) - 1
        insert_1h_interval(pair, hours)

        date_last_4h = session.query(Prices).filter(Prices.coin_id.like(coinID)).filter(Prices.interval.like('4h')).order_by(Prices.date.desc()).first().date
        hours = int(round(int((my_methods.count_hours(date_last_4h, datetime.now()))-1)/4,0))     
        insert_4h_interval(pair, hours)

        date_last_1d = session.query(Prices).filter(Prices.coin_id.like(coinID)).filter(Prices.interval.like('1d')).order_by(Prices.date.desc()).first().date
        days = int(my_methods.count_days(date_last_1d, datetime.now()))
        insert_1d_interval(pair, days)


add_missing_price()

session.commit()
session.close()