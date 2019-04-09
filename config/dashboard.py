from datetime import datetime, timedelta

DATEFORMAT = '%Y-%m-%d %H:%M:%S'
config = {

    'dates': {
        'DATEFORMAT': '%Y-%m-%d %H:%M:%S',
        'last_date': datetime.today() - timedelta(days=3),
        'DAYS_TO_LOAD':30
    }
}