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
    inflationHNT = InflationHNT(time_stamp = row['time_stamp'],
                            date = row['date'],
                            interval = row['interval'],
                            rewards = row['rewards'],
                            token_supply = row['token_supply'])
    session.add(inflationHNT)


def prepare_inflation_record_1h(api_data):
    '''
    This function will prepare data for insering
    into database table for timeframe 1h.
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


def get_last_date1h():
    '''
    Will get start date for generating data for 1h interval.
    '''
    if session.query(InflationHNT).filter(InflationHNT.interval.like('1h')).order_by(InflationHNT.date.desc()).first() is None:
        return datetime.strptime(statics.START_DAY, "%Y-%m-%d %H:%M:%S")
    else:
        return session.query(InflationHNT).filter(InflationHNT.interval.like('1h')).order_by(InflationHNT.date.desc()).first().date


def pull_data_interval_1h():
    '''
    Will pull HNT inflation data for interval 1 hour
    '''

    date_last_1h = get_last_date1h()

    # hours - 1 because of UTC
    hours = my_methods.count_hours(date_last_1h, datetime.now()) - 1

    if hours > 1:
        chunk_size = 240
        dates_list = my_methods.create_list_hours(date_last_1h, hours)
        chunked_list = [dates_list[i:i+chunk_size] for i in range(0, len(dates_list), chunk_size)]

        for chunk in chunked_list:
            query = {'min_time':chunk[0].isoformat(),
                    'max_time': my_methods.add_hour(chunk[-1]).isoformat(),
                    'bucket':'hour'}
        
            response = http.get('https://api.helium.io/v1/rewards/sum', params=query)
            response_json = response.json()
            response_json['data'].reverse()
            if len(response_json['data']) == len(chunk):
                prepare_inflation_record_1h(response_json)
                session.commit()
            else:
                for i in range (0,len(chunk)-1):
                    query = {'min_time':chunk[i].isoformat(),
                            'max_time': chunk[i+1].isoformat(),
                            'bucket':'hour'}
                    response = http.get('https://api.helium.io/v1/rewards/sum', params=query)
                    response_json = response.json()
                    prepare_inflation_record_1h(response_json)
                    session.commit()   
            session.commit()


def prepare_inflation_record_1d(api_data):
    '''
    This function will prepare data for insering
    into database table for timeframe 1d.
    '''

    min_time = datetime.fromisoformat(api_data['meta']['min_time'][:-1])
    max_time = datetime.fromisoformat(api_data['meta']['max_time'][:-1])
    days = my_methods.count_days(min_time, max_time)

    days_list = my_methods.create_list_days(max_time, days)

    for j in range(0,len(days_list)):
        record = {}

        if len(api_data['data']) == 0:
                record['rewards'] = 0
                record['token_supply'] = 0
        else:
            if 'total' in api_data['data'][j] and api_data['data'][j]['total'] != None:
                    record['rewards'] = api_data['data'][j]['total']
            else:
                record['rewards'] = 0

        record['token_supply'] = 0
        record['date'] = days_list[j]
        record['time_stamp'] = int(round(record['date'].timestamp()))
        record['interval'] = '1d'

        add_InflationHNT(record)
        print('Created record model for time stamp: {0}'.format(str(record['date'])))


def get_last_date1d():
    '''
    Will get start date for generating data for 1d interval.
    '''
    if session.query(InflationHNT).filter(InflationHNT.interval.like('1d')).order_by(InflationHNT.date.desc()).first() is None:
        return datetime.strptime(statics.START_DAY, "%Y-%m-%d %H:%M:%S")
    else:
        return session.query(InflationHNT).filter(InflationHNT.interval.like('1d')).order_by(InflationHNT.date.desc()).first().date


def pull_data_interval_1d():
    '''
    Will pull HNT inflation data for interval 1 day
    '''
    date_last_1d = get_last_date1d()

    days = my_methods.count_days(date_last_1d, datetime.now())
    
    date_list = my_methods.create_list_days(datetime.now(), days - 1)

    chunk_size = 30
    chunked_list = [date_list[i:i+chunk_size] for i in range(0, len(date_list), chunk_size)]

    if len(date_list) > 1:
        for chunk in chunked_list:

            query = {'min_time':chunk[0].isoformat() + 'Z',
                    'max_time': my_methods.add_day(chunk[-1]).isoformat() + 'Z',
                    'bucket':'day'}

            response = requests.get('https://api.helium.io/v1/rewards/sum', params=query)
            response_json = response.json()
            response_json['data'].reverse()  
            if len(response_json['data']) == len(chunk):
                prepare_inflation_record_1d(response_json)
                session.commit()
            else:
                for i in range (0,len(chunk)-1):
                    query = {'min_time':chunk[i].isoformat(),
                            'max_time': chunk[i+1].isoformat(),
                            'bucket':'day'}
                    response = http.get('https://api.helium.io/v1/rewards/sum', params=query)
                    response_json = response.json()
                    prepare_inflation_record_1d(response_json)
                    session.commit() 


def pull_HNT_inflation_data():
    '''
    Will pull data for burned DC
    '''
    pull_data_interval_1d()
    pull_data_interval_1h()


def add_first_record():
    '''
    This will add first record into database.
    '''
    row = {}
    row['time_stamp'] = 1623621600
    row['date'] = datetime.fromtimestamp(1623621600)
    row['interval']= '1d'
    row['rewards'] = 0
    row['token_supply'] = 0
    add_InflationHNT(row)
    row['interval']= '1h'
    add_InflationHNT(row)
    session.commit()

#add_first_record()

pull_HNT_inflation_data()

session.close()