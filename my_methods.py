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
        return int(divmod(duration_in_s, 86400)[0])
    else:
        print('function: count_days. Wrong parameter format. Para: {0} {1}'.format(start_date, end_date))
        return 0


def create_list_days(days_back):
    '''
    This method create list of dates.
    From today to @days_back provided.
    @days_back(integer) numer of days back
    return:
    @date_list(list) list of dates
    '''
    import datetime

    now = datetime.datetime.now().replace(hour=00, minute = 00, second = 00, microsecond = 00)

    # create list of days
    date_list = [now - datetime.timedelta(days=x) for x in range(days_back)]
    date_list.reverse()

    return date_list


def create_list_hours(day, hours):
    '''
    This method create list of hours.
    For givenday @day for number of @hours.
    @day(datetime) the day for which list will be created
    @hours(datetime) number of hours
    return:
    @hour_list(list) list of hours
    '''
    import datetime

    day += datetime.timedelta(hours=hours)
    # create list of hours
    hour_list = [day - datetime.timedelta(hours=x) for x in range(hours)]
    hour_list.reverse()

    return hour_list
