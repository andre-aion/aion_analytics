from pandas.io.json import json_normalize
from pymongo import MongoClient
from scripts.utils.mylogger import mylogger

import pandas as pd
import dask as dd
import numpy as np
from datetime import datetime, date

logger = mylogger(__file__)

class PythonMongo:
    logger = mylogger(__file__)

    # port = '9000'
    # ch = sa.create_engine('clickhouse://default:@127.0.0.1:8123/aion')
    def __init__(self, db):
        self.client = MongoClient('localhost', 27017)
        self.db = self.client['aion']
        collection = 'external'

    def load_data(self, table):
        try:
            df = pd.DataFrame(list(self.db[table].find()))
            logger.warning('external:%s',df.head(10))

            return df
        except Exception:
            logger.error('load data',exc_info=True)

    def date_to_datetime(self,timestamp):
        if isinstance(timestamp,date):
            return datetime(timestamp.year,timestamp.month,timestamp.day,0,0,0)
        return timestamp

    def load_df(self, start_date, end_date, cols=[], table='', timestamp_col='startdate_actual'):
        try:
            start_date = self.date_to_datetime(start_date)
            end_date = self.date_to_datetime(end_date)

            logger.warning('start date:enddate=%s:%s',start_date,end_date)
            df = json_normalize(list(self.db[table].find({
                timestamp_col:
                    {
                        "$gte": start_date,
                        "$lte": end_date
                    }
            },{'_id':False}).sort(timestamp_col, 1)))
            if df is not None:
                if len(df) > 0:
                    if cols is not None:
                        if len(cols) > 0:
                            df = df[cols]
                #logger.warning('df after mongo load:%s',df.head(20))
            return df
        except Exception:
            logger.error('load df', exc_info=True)
