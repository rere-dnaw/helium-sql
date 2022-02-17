# every hour

from models import InflationHNT
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


def add_InflationHNT(row):
    '''
    Add data about HNT rewars into InflationHNT table
    '''
    DCburn = InflationHNT(time_stamp = row['time_stamp'],
                    date = row['date'],
                    interval = row['interval'],
                    rewards = row['rewards'],
                    token_supply = row['token_supply'])
    session.add(DCburn)


def prepare_Inflation_record_1d(api_data):
    '''
    This function will prepare data for insering
    into table database for timeframe 1d.
    '''
    min_time = datetime.fromisoformat(api_data['meta']['min_time'][:-1])
    max_time = datetime.fromisoformat(api_data['meta']['max_time'][:-1])
    days = my_methods.count_days(min_time, max_time)

    days_list = my_methods.create_list_days(min_time, days)

    for j in range(0,len(days_list)):
        record = {}

        if len(api_data['data']) == 0:
                record['rewards'] = 0
                record['token_supply'] = 0
        else:
            if 'total' in api_data['data'][0] and api_data['data'][0]['total'] != None:
                    record['rewards'] = api_data['data'][0]['total']
            else:
                record['rewards'] = 0

        record['token_supply'] = 0
        record['date'] = datetime.fromisoformat(api_data['meta']['min_time'][:-1])
        record['time_stamp'] = int(round(record['date'].timestamp()))
        record['interval'] = '1d'

        add_InflationHNT(record)
        print('Created record model for time stamp: {0}'.format(api_data['meta']['min_time']))
 

def prepare_Inflation_record_1h(api_data):
    '''
    This function will prepare data for insering
    into table database for timeframe 1h.
    '''
    min_time = datetime.fromisoformat(api_data['meta']['min_time'][:-1])
    max_time = datetime.fromisoformat(api_data['meta']['max_time'][:-1])
    hours = my_methods.count_hours(min_time, max_time)

    hour_list = my_methods.create_list_hours(min_time, hours)

    

    for j in range(0,len(hour_list)):
        record = {}
        if 'total' in api_data['data'][j] and api_data['data'][j]['total'] != None:
            record['rewards'] = api_data['data'][j]['total']
        else:
            record['rewards'] = 0

        record['token_supply'] = 0
        record['date'] = hour_list[j]
        record['time_stamp'] = int(round(record['date'].timestamp()))
        record['interval'] = '1h'

        
        add_InflationHNT(record)
        print('Created record model for time stamp: {0}'.format(hour_list[j]))


def add_HNT_inflation_data():
    '''
    '''
    date_last_1h = ''
    date_last_1d = ''

    if session.query(InflationHNT).filter(InflationHNT.interval.like('1d')).order_by(InflationHNT.date.desc()).first() is None:
        date_last_1d = datetime.strptime(statics.START_DAY, "%Y-%m-%d %H:%M:%S")
    else:
        date_last_1d = session.query(InflationHNT).filter(InflationHNT.interval.like('1d')).order_by(InflationHNT.date.desc()).first().date

    if session.query(InflationHNT).filter(InflationHNT.interval.like('1h')).order_by(InflationHNT.date.desc()).first() is None:
        date_last_1h = datetime.strptime(statics.START_DAY, "%Y-%m-%d %H:%M:%S")
    else:
        date_last_1h = session.query(InflationHNT).filter(InflationHNT.interval.like('1h')).order_by(InflationHNT.date.desc()).first().date

    # hours - 1 because of UTC
    hours = my_methods.count_hours(date_last_1h, datetime.now()) - 1

    days = my_methods.count_days(date_last_1d, datetime.now())
    
    date_list = my_methods.create_list_days(datetime.now(), days)

    chunk_size = 2
    chunked_list = [date_list[i:i+chunk_size] for i in range(0, len(date_list), chunk_size)]

    if len(date_list) > 1:
        for chunk in chunked_list:
            query = {'min_time':chunk[0].isoformat(),
                    'max_time': chunk[-1].isoformat(),
                    'bucket':'day'}
            response = http.get('https://api.helium.io/v1/rewards/sum', params=query)
            response_json = response.json()
            response_json['data'].reverse()   
            prepare_Inflation_record_1d(response_json)
            session.commit()        

    if hours > 1:
        chunk_size = 200
        dates_list = my_methods.create_list_hours(date_last_1h, hours)
        chunked_list = [dates_list[i:i+chunk_size] for i in range(0, len(dates_list), chunk_size)]

        for chunk in chunked_list:

            query = {'min_time':chunk[0].isoformat(),
                    'max_time': chunk[-1].isoformat(),
                    'bucket':'hour'}
        
            response = http.get('https://api.helium.io/v1/rewards/sum', params=query)
            response_json = response.json()

            response_json['data'].reverse()   

            prepare_Inflation_record_1h(response_json)
            
            session.commit()


add_HNT_inflation_data()

session.close()