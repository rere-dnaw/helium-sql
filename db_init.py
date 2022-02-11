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

def init_db():
    '''
    This method will fill db table 
    '''

    hours = int(my_methods.count_hours(datetime.strptime(statics.START_DAY_BINANCE_DATA, "%Y-%m-%d %H:%M:%S"),
                                datetime.now()))

    days = int(my_methods.count_days(datetime.strptime(statics.START_DAY_BINANCE_DATA, "%Y-%m-%d %H:%M:%S"),
                                datetime.now()))
 

    for pair in statics.TOKEN_LIST:
        add_coin(pair)
        session.commit()


        pairID = get_coin_id(pair)
        
        
        interval = '1h'
        data = exchange.fetch_ohlcv(pair, interval, limit=hours)
        for row in data:
            add_price(row, pairID, interval)
        session.commit()


        interval = '4h'
        data = exchange.fetch_ohlcv(pair, interval, limit=int(round(hours/4,0)))
        for row in data:
            add_price(row, pairID, interval)
        session.commit()


        interval = '1d'
        data = exchange.fetch_ohlcv(pair, interval, limit=days)
        for row in data:
            add_price(row, pairID, interval)
        session.commit()
            


init_db()



session.commit()
session.close()