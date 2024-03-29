## run every hour

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
    if session.query(Coins).with_entities(Coins.id) \
            .filter(Coins.name.like(pair)).first() is None:
        return None
    else:
        pair_id = session.query(Coins).with_entities(Coins.id) \
                    .filter(Coins.name.like(pair)).all()
        return [item[0] for item in pair_id][0]


def pull_data_for_interval(pair, interval, limit):
    '''
    '''
    pairID = get_coin_id(pair)
    data = exchange.fetch_ohlcv(pair, interval, limit=limit)
    for row in data:
        add_price(row, pairID, interval)
    session.commit()


def get_last_date(coin_id, interval):
    '''
    Will get start date for generating data for provided interval.
    '''
    if coin_id is not None:
        if session.query(Prices).filter(Prices.coin_id.like(coin_id)).filter(Prices.interval.like(interval)).order_by(Prices.date.desc()).first() is None:
            return datetime.strptime(statics.START_DAY, "%Y-%m-%d %H:%M:%S")
        else:
            return session.query(Prices).filter(Prices.coin_id.like(coin_id)).filter(Prices.interval.like(interval)).order_by(Prices.date.desc()).first().date
    else:
        return datetime.strptime(statics.START_DAY, "%Y-%m-%d %H:%M:%S")


def add_coin_price_data():
    '''
    '''
    for pair in statics.TOKEN_LIST:
        coin_id = get_coin_id(pair)
        date_last_1d = get_last_date(coin_id, '1d')
        print(date_last_1d)
        date_last_1h = get_last_date(coin_id, '1h')
        
        print(date_last_1h)

        date_last_4h = get_last_date(coin_id, '4h')
        print(date_last_4h)



        print(datetime.utcnow())
        days = my_methods.count_days(date_last_1d, datetime.utcnow())
        hours = my_methods.count_hours(date_last_1h, datetime.utcnow())
        hours4 = my_methods.count_hours(date_last_4h, datetime.utcnow())

        add_coin(pair)
        session.commit()

        if days != 0:
            pull_data_for_interval(pair, '1d', days)

        if hours != 0:
            pull_data_for_interval(pair, '1h', hours)

        if hours4 >= 4:
            pull_data_for_interval(pair, '4h', int(round(hours4/4,0)))


add_coin_price_data()

session.commit()
session.close()