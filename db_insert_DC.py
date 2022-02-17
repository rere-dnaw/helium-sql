# every hour

from models import BurnedDC
from base import Session, engine, Base
from datetime import datetime
import my_methods
import requests
from requests.adapters import HTTPAdapter, Retry


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


def add_DCburn(row):
    '''
    '''
    DCburn = BurnedDC(time_stamp = row['time_stamp'],
                    date = row['date'],
                    interval = row['interval'],
                    state_channel = row['state_channel'],
                    fee = row['fee'],
                    assert_location = row['assert_location'],
                    add_gateway = row['add_gateway'],
                    total = row['total'])
    session.add(DCburn)


def prepare_DC_record_1d(api_data):
    '''
    This function will prepare data for insering
    into table database for timeframe 1d.
    '''
    record = {}
    if len(api_data['data']) == 0:
        record['total'] = 0
        record['state_channel'] = 0
        record['fee'] = 0
        record['assert_location'] = 0
        record['add_gateway'] = 0
    else:
        if 'total' in api_data['data'][0] and api_data['data'][0]['total'] != None:
            record['total'] = api_data['data'][0]['total']
        else:
            record['total'] = 0

        if 'state_channel' in api_data['data'][0] and api_data['data'][0]['state_channel'] != None:
            record['state_channel'] = api_data['data'][0]['state_channel']
        else:
            record['state_channel'] = 0

        if 'fee' in api_data['data'][0] and api_data['data'][0]['fee'] != None:
            record['fee'] = api_data['data'][0]['fee']
        else:
            record['fee'] = 0

        if 'assert_location' in api_data['data'][0] and api_data['data'][0]['assert_location'] != None:
            record['assert_location'] = api_data['data'][0]['assert_location']
        else:
            record['assert_location'] = 0

        if 'add_gateway' in api_data['data'][0] and api_data['data'][0]['add_gateway'] != None:
            record['add_gateway'] = api_data['data'][0]['add_gateway']
        else:
            record['add_gateway'] = 0

    record['date'] = datetime.fromisoformat(api_data['meta']['min_time'][:-1])
    record['time_stamp'] = int(round(record['date'].timestamp()))
    record['interval'] = '1d'

    add_DCburn(record)

        

def prepare_DC_record_1h(api_data):
    '''
    This function will prepare data for insering
    into table database for timeframe 1d.
    '''
    min_time = datetime.fromisoformat(api_data['meta']['min_time'][:-1])
    max_time = datetime.fromisoformat(api_data['meta']['max_time'][:-1])
    hours = my_methods.count_hours(min_time, max_time)

    hour_list = my_methods.create_list_hours(min_time, hours)

    for i in range(0,len(hour_list)):
        record = {}
        if 'total' in api_data['data'][i] and api_data['data'][i]['total'] != None:
            record['total'] = api_data['data'][i]['total']
        else:
            record['total'] = 0

        if 'state_channel' in api_data['data'][i] and api_data['data'][i]['state_channel'] != None:
            record['state_channel'] = api_data['data'][i]['state_channel']
        else:
            record['state_channel'] = 0

        if 'fee' in api_data['data'][i] and api_data['data'][i]['fee'] != None:
            record['fee'] = api_data['data'][i]['fee']
        else:
            record['fee'] = 0

        if 'assert_location' in api_data['data'][i] and api_data['data'][i]['assert_location'] != None:
            record['assert_location'] = api_data['data'][i]['assert_location']
        else:
            record['assert_location'] = 0

        if 'add_gateway' in api_data['data'][i] and api_data['data'][i]['add_gateway'] != None:
            record['add_gateway'] = api_data['data'][i]['add_gateway']
        else:
            record['add_gateway'] = 0

        record['date'] = hour_list[i]
        record['time_stamp'] = int(round(record['date'].timestamp()))
        record['interval'] = '1h'
        
        add_DCburn(record)
        print('Created record model for time stamp: {0}'.format(hour_list[i]))



def add_missing_price():
    '''
    '''

    date_last_1h = session.query(BurnedDC).filter(BurnedDC.interval.like('1h')).order_by(BurnedDC.date.desc()).first().date
    # hours - 1 because of UTC
    hours = int(my_methods.count_hours(date_last_1h, datetime.now())) - 1

    date_last_1d = session.query(BurnedDC).filter(BurnedDC.interval.like('1d')).order_by(BurnedDC.date.desc()).first().date
    days = int(my_methods.count_days(date_last_1d, datetime.now()))
    

    date_list = my_methods.create_list_days(days)


    if len(date_list) > 1:
        for i in range (0,len(date_list)-1):
            query = {'min_time':date_list[i].isoformat(),
                    'max_time': date_list[i+1].isoformat(),
                    'bucket':'day'}

            response = http.get('https://api.helium.io/v1/dc_burns/sum', params=query)
            response_json = response.json()
            prepare_DC_record_1d(response_json)
        session.commit()        
    
    max_time = datetime.now().replace(minute = 00, second = 00, microsecond = 00)

    if hours > 1:
        chunk_size = 200
        dates_list = my_methods.create_list_hours(date_last_1h, hours)
        chunked_list = [dates_list[i:i+chunk_size] for i in range(0, len(dates_list), chunk_size)]

        for chunk in chunked_list:


            query = {'min_time':chunk[0].isoformat(),
                    'max_time': chunk[-1].isoformat(),
                    'bucket':'hour'}
        
            response = http.get('https://api.helium.io/v1/dc_burns/sum', params=query)
            response_json = response.json()

            

            response_json['data'].reverse()   

            prepare_DC_record_1h(response_json)
            
            session.commit()

        


add_missing_price()

session.close()