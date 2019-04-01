from datetime import datetime, timedelta

DATEFORMAT = '%Y-%m-%d %H:%M:%S'
config = {

    'dates': {
        'DATEFORMAT': '%Y-%m-%d %H:%M:%S',
        'last_date': datetime.strptime("2018-10-24 00:00:00", DATEFORMAT),
        'DAYS_TO_LOAD':30
    }
}