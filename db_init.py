from models import FearGreed, Prices, Coins, BurnedDC, InflationHNT
from base import Session, engine, Base
import statics
import ccxt
from datetime import datetime
import my_methods
import requests
from requests.adapters import HTTPAdapter, Retry



exchange = ccxt.binance({
    'apiKey': statics.BINANCE_API_KEY,
    'secret': statics.BINANCE_SECRET_KEY
})


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


def add_price(row, pairID, interval):
    '''
    This method will add token info into db.
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
    This method will add token pait into coin table.
    '''
    pairs = [item[0] for item in session.query(Coins).with_entities(Coins.name).all()]
    if coin not in pairs:
        session.add(Coins(name = coin))


def get_coin_id(pair):
    '''
    The function will return
    id for provided token pair.
    '''
    pair_id = session.query(Coins).with_entities(Coins.id) \
                .filter(Coins.name.like(pair)).all()
    return [item[0] for item in pair_id][0]


def add_feergreed(row):
    '''
    Will add fear and greed index balue into FearGreed table.
    '''
    row['date'] = datetime.fromtimestamp(int(row['timestamp'])).strftime('%Y-%m-%d %H:%M:%S')
    indexFG = FearGreed(time_stamp = int(row['timestamp']),
                        date = datetime.strptime(row['date'], "%Y-%m-%d %H:%M:%S"),
                        value = int(row['value']),
                        classification= row['value_classification'])
    session.add(indexFG)


def add_DCburn(row):
    '''
    Add data about dc burned into BurnedDC table
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


def prepare_Inflation_record_1d(api_data):
    '''
    This function will prepare data for insering
    into table database for timeframe 1d.
    '''

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


def prepare_DC_record_1h(api_data):
    '''
    This function will prepare data for insering
    into table database for timeframe 1d.
    '''

    record = {}

    hour_list = my_methods.create_list_hours(datetime.fromisoformat(api_data['meta']['min_time'][:-1]), 24)

    if len(api_data['data']) == 0:
            for j in range(0,len(hour_list)):
                record = {}
                record['total'] = 0
                record['state_channel'] = 0
                record['fee'] = 0
                record['assert_location'] = 0
                record['add_gateway'] = 0
                record['date'] = hour_list[j]
                record['time_stamp'] = int(round(record['date'].timestamp()))
                record['interval'] = '1h'
                add_DCburn(record)
    else:
        record_list = api_data['data']
        record_list.reverse()

        for j in range(0,len(hour_list)):
            record = {}
            if 'total' in record_list[j] and record_list[j]['total'] != None:
                record['total'] = record_list[j]['total']
            else:
                record['total'] = 0

            if 'state_channel' in record_list[j] and record_list[j]['state_channel'] != None:
                record['state_channel'] = record_list[j]['state_channel']
            else:
                record['state_channel'] = 0

            if 'fee' in record_list[j] and record_list[j]['fee'] != None:
                record['fee'] = record_list[j]['fee']
            else:
                record['fee'] = 0

            if 'assert_location' in record_list[j] and record_list[j]['assert_location'] != None:
                record['assert_location'] = record_list[j]['assert_location']
            else:
                record['assert_location'] = 0

            if 'add_gateway' in record_list[j] and record_list[j]['add_gateway'] != None:
                record['add_gateway'] = record_list[j]['add_gateway']
            else:
                record['add_gateway'] = 0

            record['date'] = hour_list[j]
            record['time_stamp'] = int(round(record['date'].timestamp()))
            record['interval'] = '1h'
            add_DCburn(record)
    
    session.commit()


def prepare_Inflation_record_1h(api_data):
    '''
    This function will prepare data for insering
    into table database for timeframe 1d.
    '''

    hour_list = my_methods.create_list_hours(datetime.fromisoformat(api_data['meta']['min_time'][:-1]), 24)

    if len(api_data['data']) == 0:
            for j in range(0,len(hour_list)):
                record['rewards'] = 0
                record['token_supply'] = 0
                add_InflationHNT(record)
    else:
        record_list = api_data['data']
        record_list.reverse()

        for j in range(0,len(hour_list)):
            record = {}
            if 'total' in record_list[j] and record_list[j]['total'] != None:
                record['rewards'] = record_list[j]['total']
            else:
                record['rewards'] = 0

            record['token_supply'] = 0
            record['date'] = hour_list[j]
            record['time_stamp'] = int(round(record['date'].timestamp()))
            record['interval'] = '1h'
            add_InflationHNT(record)
    
    session.commit()


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


def init_db():
    '''
    This method will fill db table 
    '''

    hours = int(my_methods.count_hours(datetime.strptime(statics.START_DAY, "%Y-%m-%d %H:%M:%S"),
                                datetime.now()))

    days = int(my_methods.count_days(datetime.strptime(statics.START_DAY, "%Y-%m-%d %H:%M:%S"),
                                datetime.now()))
 

    fill_db(days, hours)


def fill_db(days, hours):
    '''
    '''
    for pair in statics.TOKEN_LIST:
        add_coin(pair)
        session.commit()

        insert_1h_interval(pair, hours)
        
        insert_4h_interval(pair, int(round(hours/4,0)))

        insert_1d_interval(pair, days)

    fear_gree_index = http.get('https://api.alternative.me/fng/?limit={0}'.format(days)).json()
    for fear_greed_val in fear_gree_index['data']:
        add_feergreed(fear_greed_val)



    # create list of days
    date_list = my_methods.create_list_days(days)

    for i in range (0,len(date_list)-1):
        query = {'min_time':date_list[i].isoformat(),
                'max_time': date_list[i+1].isoformat(),
                'bucket':'day'}

        response = http.get('https://api.helium.io/v1/dc_burns/sum', params=query)
        print(response.status_code)
        response_inflation = http.get('https://api.helium.io/v1/rewards/sum', params=query)
        print(response_inflation.status_code)

        response_json = response.json()
        response_inflation_json = response_inflation.json()

        prepare_DC_record_1d(response_json)
        prepare_Inflation_record_1d(response_inflation_json)
        

        query = {'min_time':date_list[i].isoformat(),
                'max_time': date_list[i+1].isoformat(),
                'bucket':'hour'}
    
        response = http.get('https://api.helium.io/v1/dc_burns/sum', params=query)
        print(response.status_code)
        response_inflation = http.get('https://api.helium.io/v1/rewards/sum', params=query)
        print(response_inflation.status_code)

        response_json = response.json()
        response_inflation_json = response_inflation.json()

        prepare_DC_record_1h(response_json)
        prepare_Inflation_record_1h(response_inflation_json)
  

    session.commit()


def insert_1h_interval(pair, hours):
    '''
    '''
    pairID = get_coin_id(pair)
    interval = '1h'
    data = exchange.fetch_ohlcv(pair, interval, limit=hours)
    for row in data:
        add_price(row, pairID, interval)
    session.commit()


def insert_4h_interval(pair, hours):
    '''
    '''
    pairID = get_coin_id(pair)
    interval = '4h'
    data = exchange.fetch_ohlcv(pair, interval, limit=hours)
    for row in data:
        add_price(row, pairID, interval)
    session.commit()


def insert_1d_interval(pair, days):
    '''
    '''
    pairID = get_coin_id(pair)
    interval = '1d'
    data = exchange.fetch_ohlcv(pair, interval, limit=days)
    for row in data:
        add_price(row, pairID, interval)
    session.commit()


if session.query(Coins).first() is None:
    init_db()


session.commit()
session.close()