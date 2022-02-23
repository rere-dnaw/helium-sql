# every hour

from models import RewardsHNT
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


def add_reward_HNT(row):
    '''
    Add data about HNT rewars into RewardsHNT table
    '''
    rewardHNT = RewardsHNT(time_stamp = row['time_stamp'],
                            date = row['date'],
                            interval = row['interval'],
                            rewards = row['rewards'])
    session.add(rewardHNT)
    

def get_last_date(interval):
    '''
    Will get start date for generating data for provided interval.
    '''
    if session.query(RewardsHNT).filter(RewardsHNT.interval.like(interval)).order_by(RewardsHNT.date.desc()).first() is None:
        return datetime.strptime(statics.START_DAY, "%Y-%m-%d %H:%M:%S")
    else:
        return session.query(RewardsHNT).filter(RewardsHNT.interval.like(interval)).order_by(RewardsHNT.date.desc()).first().date


def prepare_reward_record_1h(api_data):
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

        record['date'] = hour_list[j]
        record['time_stamp'] = int(round(record['date'].timestamp()))
        record['interval'] = '1h'
        
        add_reward_HNT(record)
        print('Created record model for time stamp: {0}'.format(hour_list[j]))


def pull_data_interval_1h():
    '''
    Will pull HNT reward data for interval 1 hour
    '''

    date_last_1h = get_last_date('1h')

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
                prepare_reward_record_1h(response_json)
                session.commit()
            else:
                for i in range (0,len(chunk)-1):
                    query = {'min_time':chunk[i].isoformat(),
                            'max_time': chunk[i+1].isoformat(),
                            'bucket':'hour'}
                    response = http.get('https://api.helium.io/v1/rewards/sum', params=query)
                    response_json = response.json()
                    prepare_reward_record_1h(response_json)
                    session.commit()   
            session.commit()


def prepare_reward_record_1d(api_data):
    '''
    This function will prepare data for insering
    into database table for timeframe 1d.
    '''
    api_data['data'] = [api_data['data']]
    
    min_time = datetime.fromisoformat(api_data['meta']['min_time'][:-1])
    max_time = datetime.fromisoformat(api_data['meta']['max_time'][:-1])
    days = my_methods.count_days(min_time, max_time)

    days_list = my_methods.create_list_days(max_time, days)

    for j in range(0,len(days_list)):
        record = {}

        if len(api_data['data']) == 0:
                record['rewards'] = 0
        else:
            if 'total' in api_data['data'][j] and api_data['data'][j]['total'] != None:
                    record['rewards'] = api_data['data'][j]['total']
            else:
                record['rewards'] = 0

        record['date'] = days_list[j]
        record['time_stamp'] = int(round(record['date'].timestamp()))
        record['interval'] = '1d'

        add_reward_HNT(record)
        print('Created record model for time stamp: {0}'.format(str(record['date'])))


def pull_data_interval_1d():
    '''
    Will pull HNT reward data for interval 1 day
    '''
    date_last_1d = get_last_date('1d')

    days = my_methods.count_days(date_last_1d, datetime.now())
    
    date_list = my_methods.create_list_days(datetime.now(), days - 1)

    if len(date_list) > 1:
        for i in range (0,len(date_list)-1):
            query = {'min_time':date_list[i].isoformat(),
                    'max_time': date_list[i+1].isoformat()}

            response = http.get('https://api.helium.io/v1/rewards/sum', params=query)
            response_json = response.json()
            prepare_reward_record_1d(response_json)
            session.commit()
    if len(date_list) == 1:
        query = {'min_time':date_list[0].isoformat(),
                 'max_time': my_methods.add_day(date_list[0]).isoformat()}

        response = http.get('https://api.helium.io/v1/rewards/sum', params=query)
        response_json = response.json()
        prepare_reward_record_1d(response_json)
        session.commit()


def pull_HNT_reward_data():
    '''
    Will pull data for burned DC
    '''
    pull_data_interval_1d()
    #pull_data_interval_1h()

#112605262.7288517
#112605250.72205125
def add_first_record():
    '''
    This will add first record into database.
    '''
    row = {}
    row['time_stamp'] = 1623621600
    row['date'] = datetime.fromtimestamp(1623621600)
    row['interval']= '1d'
    row['rewards'] = 0
    add_reward_HNT(row)
    row['interval']= '1h'
    add_reward_HNT(row)
    session.commit()

#add_first_record()

pull_HNT_reward_data()

session.close()