from functools import partial
from models import Prices, Coins, FearGreed
from base import Session, engine, Base
import statics
import ccxt
from datetime import datetime
import my_methods
import requests



Base.metadata.create_all(engine)

session = Session()


def add_feergreed(row):
    '''
    '''
    row['date'] = datetime.fromtimestamp(int(row['timestamp'])).strftime('%Y-%m-%d %H:%M:%S')
    indexFG = FearGreed(time_stamp = int(row['timestamp']),
                        date = datetime.strptime(row['date'], "%Y-%m-%d %H:%M:%S"),
                        value = int(row['value']),
                        classification= row['value_classification'])
    session.add(indexFG)


def add_missing_FG_value():
    '''
    '''

    date_last_1d = session.query(FearGreed).order_by(FearGreed.date.desc()).first().date
    days = int(my_methods.count_days(date_last_1d, datetime.now()))

    if days != 0:
        fear_gree_index = requests.get('https://api.alternative.me/fng/?limit={0}'.format(days)).json()
        for fear_greed_val in fear_gree_index['data']:
            add_feergreed(fear_greed_val)
        session.commit()
    


add_missing_FG_value()

session.commit()
session.close()