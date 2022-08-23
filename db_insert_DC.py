# every hour

from models import BurnedDC
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


def add_DCburn(row):
    '''
    Is adding data into data base model
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


def get_last_date(interval):
    '''
    Will get start date for generating data for provided interval.
    '''
    if session.query(BurnedDC).filter(BurnedDC.interval.like(interval)).order_by(BurnedDC.date.desc()).first() is None:
        return datetime.strptime(statics.START_DAY, "%Y-%m-%d %H:%M:%S")
    else:
        return session.query(BurnedDC).filter(BurnedDC.interval.like(interval)).order_by(BurnedDC.date.desc()).first().date


def prepare_DC_record_1h(api_data):
    '''
    This function will prepare data for insering
    into database table for timeframe 1h.
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


def pull_data_interval_1h():
    '''
    Will pull DC data for interval 1 hour
    '''

    date_last_1h = get_last_date('1h')

    # hours - 1 because of UTC
    hours = my_methods.count_hours(date_last_1h, datetime.utcnow()) - 1

    if hours > 1:
        chunk_size = 240
        dates_list = my_methods.create_list_hours(date_last_1h, hours)
        chunked_list = [dates_list[i:i+chunk_size] for i in range(0, len(dates_list), chunk_size)]

        for chunk in chunked_list:
            query = {'min_time':chunk[0].isoformat(),
                    'max_time': my_methods.add_hour(chunk[-1]).isoformat(),
                    'bucket':'hour'}
        
            response = http.get('https://api.helium.io/v1/dc_burns/sum', params=query)
            response_json = response.json()
            response_json['data'].reverse()
            if len(response_json['data']) == len(chunk):
                prepare_DC_record_1h(response_json)
                session.commit()
            else:
                chunk.append(my_methods.add_hour(chunk[-1]))
                for i in range (0,len(chunk)-1):
                    query = {'min_time':chunk[i].isoformat(),
                            'max_time': chunk[i+1].isoformat(),
                            'bucket':'hour'}
                    response = http.get('https://api.helium.io/v1/dc_burns/sum', params=query)
                    response_json = response.json()
                    prepare_DC_record_1h(response_json)
                    session.commit()   
            session.commit()


def prepare_DC_record_1d(api_data):
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
            record['total'] = 0
            record['state_channel'] = 0
            record['fee'] = 0
            record['assert_location'] = 0
            record['add_gateway'] = 0
        else:
            if 'total' in api_data['data'][j] and api_data['data'][j]['total'] != None:
                record['total'] = api_data['data'][j]['total']
            else:
                record['total'] = 0

            if 'state_channel' in api_data['data'][j] and api_data['data'][j]['state_channel'] != None:
                record['state_channel'] = api_data['data'][j]['state_channel']
            else:
                record['state_channel'] = 0

            if 'fee' in api_data['data'][j] and api_data['data'][j]['fee'] != None:
                record['fee'] = api_data['data'][j]['fee']
            else:
                record['fee'] = 0

            if 'assert_location' in api_data['data'][j] and api_data['data'][j]['assert_location'] != None:
                record['assert_location'] = api_data['data'][j]['assert_location']
            else:
                record['assert_location'] = 0

            if 'add_gateway' in api_data['data'][j] and api_data['data'][j]['add_gateway'] != None:
                record['add_gateway'] = api_data['data'][j]['add_gateway']
            else:
                record['add_gateway'] = 0

        record['date'] = days_list[j]
        record['time_stamp'] = int(round(record['date'].timestamp()))
        record['interval'] = '1d'

        add_DCburn(record)
        print('Created record model for time stamp: {0}'.format(str(record['date'])))


def pull_data_interval_1d():
    '''
    Will pull DC data for interval 1 day
    '''
    date_last_1d = get_last_date('1d')

    days = my_methods.count_days(date_last_1d, datetime.utcnow())
    
    date_list = my_methods.create_list_days(datetime.utcnow(), days - 1)

    chunk_size = 30
    chunked_list = [date_list[i:i+chunk_size] for i in range(0, len(date_list), chunk_size)]

    if len(date_list) > 1:
        for chunk in chunked_list:

            query = {'min_time':chunk[0].isoformat(),
                    'max_time': my_methods.add_day(chunk[-1]).isoformat(),
                    'bucket':'day'}

            response = http.get('https://api.helium.io/v1/dc_burns/sum', params=query)
            response_json = response.json()
            response_json['data'].reverse()  
            if len(response_json['data']) == len(chunk):
                prepare_DC_record_1d(response_json)
                session.commit()
            else:
                chunk.append(my_methods.add_day(chunk[-1]))
                for i in range (0,len(chunk)-1):
                    query = {'min_time':chunk[i].isoformat(),
                            'max_time': chunk[i+1].isoformat(),
                            'bucket':'day'}
                    response = http.get('https://api.helium.io/v1/dc_burns/sum', params=query)
                    response_json = response.json()
                    prepare_DC_record_1d(response_json)
                    session.commit()

    if len(date_list) == 1:
        query = {'min_time':date_list[0].isoformat(),
                'max_time': my_methods.add_day(date_list[0]).isoformat(),
                'bucket':'day'}
        response = http.get('https://api.helium.io/v1/dc_burns/sum', params=query)
        response_json = response.json()
        response_json['data'].reverse()  
        prepare_DC_record_1d(response_json)
        session.commit()


def pull_data_DC_burned():
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
    row['time_stamp'] = 1623708000
    row['date'] = datetime.fromtimestamp(1623708000)
    row['interval']= '1d'
    row['state_channel'] = 77263
    row['fee'] = 21710000
    row['assert_location'] = 140000000
    row['add_gateway'] = 420000000
    row['total'] = 581787263
    add_DCburn(row)
    row['interval']= '1h'
    add_DCburn(row)
    session.commit()

#add_first_record()
pull_data_DC_burned()

session.close()