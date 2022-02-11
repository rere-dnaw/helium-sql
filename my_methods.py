from datetime import datetime


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