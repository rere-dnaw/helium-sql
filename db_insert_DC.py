from functools import partial
from models import BurnedDC
from base import Session, engine, Base
import statics
import ccxt
from datetime import datetime
import my_methods
import requests


Base.metadata.create_all(engine)

session = Session()


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

            response = requests.get('https://api.helium.io/v1/dc_burns/sum', params=query)
            response_json = response.json()
            record = response_json['data'][0]
            record['date'] = datetime.fromisoformat(response_json['meta']['min_time'][:-1])
            record['time_stamp'] = int(round(record['date'].timestamp()))
            record['interval'] = '1d'


        add_DCburn(record)
        

    max_time = datetime.now().replace(minute = 00, second = 00, microsecond = 00)

    if hours > 1:
        query = {'min_time':date_last_1h.isoformat(),
                'max_time': max_time.isoformat(),
                'bucket':'hour'}
    
        response = requests.get('https://api.helium.io/v1/dc_burns/sum', params=query)
        response_json = response.json()

        record_list = response_json['data']
        record_list.reverse()

        hour_list = my_methods.create_list_hours(datetime.fromisoformat(response_json['meta']['min_time'][:-1]), hours)

        for i in range(0,len(hour_list)):
            record = {}
            if 'total' in record_list[i] and record_list[i]['total'] != None:
                record['total'] = record_list[i]['total']
            else:
                record['total'] = 0

            if 'state_channel' in record_list[i] and record_list[i]['state_channel'] != None:
                record['state_channel'] = record_list[i]['state_channel']
            else:
                record['state_channel'] = 0

            if 'fee' in record_list[i] and record_list[i]['fee'] != None:
                record['fee'] = record_list[i]['fee']
            else:
                record['fee'] = 0

            if 'assert_location' in record_list[i] and record_list[i]['assert_location'] != None:
                record['assert_location'] = record_list[i]['assert_location']
            else:
                record['assert_location'] = 0

            if 'add_gateway' in record_list[i] and record_list[i]['add_gateway'] != None:
                record['add_gateway'] = record_list[i]['add_gateway']
            else:
                record['add_gateway'] = 0

            record['date'] = hour_list[i]
            record['time_stamp'] = int(round(record['date'].timestamp()))
            record['interval'] = '1h'
            add_DCburn(record)





add_missing_price()

session.commit()
session.close()