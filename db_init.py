from models import BTC_1D, BTC_1H, BTC_4H
from base import Session, engine, Base
import statics
import ccxt
from datetime import datetime


def fill_db_table(session, pair, timeframe, record_number):
    '''
    This method will fill db table 
    '''

    data = exchange.fetch_ohlcv(pair, timeframe, limit=record_number)

    for row in data:
        row.insert(1, datetime.utcfromtimestamp(int(row[0]/1000)).strftime('%Y-%m-%d %H:%M:%S'))
        record = None

        if timeframe == '1h':
            record = BTC_1H(row[0],
                            datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S"),
                            row[2],
                            row[3],
                            row[4],
                            row[5],
                            row[6])
        elif timeframe == '4h':
            record = BTC_4H(row[0],
                            datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S"),
                            row[2],
                            row[3],
                            row[4],
                            row[5],
                            row[6])
        else:
            record = BTC_1D(row[0],
                            datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S"),
                            row[2],
                            row[3],
                            row[4],
                            row[5],
                            row[6])


        session.add(record)


def count_hours(start_date, end_date):
    '''
    The date format must be: %Y-%m-%d %H:%M:%S
    e.g. 2022-01-11 00:00:00
    parameters:
    @start_date(string) date in correct format
    @end_date(string) date in correct format
    return:
    @hours(int) number of hours
    '''

    if (type(start_date) is datetime and type(end_date) is datetime):
        duration_in_s =  (end_date - start_date).total_seconds()
        return divmod(duration_in_s, 3600)[0]
    else:
        print('function: count_hours. Wrong parameter format. Para: {0} {1}'.format(start_date, end_date))
        return 0


def count_days(start_date, end_date):
    '''
    The date format must be: %Y-%m-%d %H:%M:%S
    e.g. 2022-01-11 00:00:00
    parameters:
    @start_date(string) date in correct format
    @end_date(string) date in correct format
    return:
    @days(int) number of days
    '''

    if (type(start_date) is datetime and type(end_date) is datetime):
        duration_in_s =  (end_date - start_date).total_seconds()
        return divmod(duration_in_s, 86400)[0]
    else:
        print('function: count_days. Wrong parameter format. Para: {0} {1}'.format(start_date, end_date))
        return 0
         


exchange = ccxt.binance({
    'apiKey': statics.BINANCE_API_KEY,
    'secret': statics.BINANCE_SECRET_KEY
})


Base.metadata.create_all(engine)

session = Session()
hours_number = int(count_hours(datetime.strptime(statics.START_DAY_BINANCE_DATA, "%Y-%m-%d %H:%M:%S"),
                                datetime.now()))

days_number = int(count_days(datetime.strptime(statics.START_DAY_BINANCE_DATA, "%Y-%m-%d %H:%M:%S"),
                                datetime.now()))

for token in statics.TOKEN_LIST:
    fill_db_table(session, token, '1h', hours_number)
    fill_db_table(session, token, '4h', int(round(hours_number/4,0)))
    fill_db_table(session, token, '1d', days_number)


session.commit()
session.close()