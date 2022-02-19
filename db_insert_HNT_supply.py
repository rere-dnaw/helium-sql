# every hour

from models import TokenSupplyHNT
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


def add_supply_HNT(row):
    '''
    Add data about HNT supply into TokenSupplyHNT model
    '''
    supplyHNT = TokenSupplyHNT(time_stamp = row['time_stamp'],
                            date = row['date'],
                            supply_amount = row['supply_amount'])
    session.add(supplyHNT)
    

def prepare_supply_record(api_data):
    '''
    This function will prepare data for insering
    into database table.
    '''
    record = {}


    record['date'] = datetime.now()
    record['time_stamp'] = int(round(record['date'].timestamp()))
    record['supply_amount'] = api_data['data']['token_supply']


    add_supply_HNT(record)


def pull_HNT_pull_supply_HNT():
    '''
    Will pull HNT supply data for current time.
    '''

    response = http.get('https://api.helium.io/v1/stats/token_supply')
    response_json = response.json()
    prepare_supply_record(response_json)
    session.commit()


pull_HNT_pull_supply_HNT()

session.close()