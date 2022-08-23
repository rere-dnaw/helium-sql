# run everyday 3:00AM

from models import FearGreed
from base import Session, engine, Base
from datetime import datetime
import my_methods
import requests
from requests.adapters import HTTPAdapter, Retry
import statics


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


def add_feergreed(row):
    '''
    This method will insert row to data base model
    '''
    row['date'] = datetime.fromtimestamp(int(row['timestamp'])).strftime('%Y-%m-%d %H:%M:%S')
    indexFG = FearGreed(time_stamp = int(row['timestamp']),
                        date = datetime.strptime(row['date'], "%Y-%m-%d %H:%M:%S"),
                        value = int(row['value']),
                        classification= row['value_classification'])
    session.add(indexFG)


def get_last_date():
    '''
    Will get start date for generating data for 1d interval.
    '''
    if session.query(FearGreed).order_by(FearGreed.date.desc()).first() is None:
        return datetime.strptime(statics.START_DAY, "%Y-%m-%d %H:%M:%S")
    else:
        return session.query(FearGreed).order_by(FearGreed.date.desc()).first().date


def pull_fear_greed_data():
    '''
    Will pull data for fear and greed index.
    '''
    date_last_1d = get_last_date()
    days = int(my_methods.count_days(date_last_1d, datetime.utcnow()))
    if days != 0:
        fear_gree_index = http.get('https://api.alternative.me/fng/?limit={0}'.format(days)).json()
        fear_gree_index['data'].reverse()
        for fear_greed_val in fear_gree_index['data']:
            add_feergreed(fear_greed_val)
    session.commit()


pull_fear_greed_data()

session.close()