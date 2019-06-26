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

    def load_df(self, start_date, end_date, cols=[], table='', timestamp_col='startdate_actual',
                supplemental_where=None):
        try:
            start_date = self.date_to_datetime(start_date)
            end_date = self.date_to_datetime(end_date)

            logger.warning('start date:enddate=%s:%s',start_date,end_date)

            if len(cols) > 0:
                cols_to_load = {}
                cols_to_load['_id'] = 0
                for col in cols:
                    cols_to_load[col] = 1
                    #logger.warning('LINE 47:%s',col)
                logger.warning('table:%s', table)
                logger.warning('timestamp col:%s', timestamp_col)
                df = json_normalize(list(self.db[table].find({
                    timestamp_col:
                        {
                            "$gte": start_date,
                            "$lte": end_date
                        }
                },cols_to_load)))
                logger.warning('LIne 56: %s',df.head(20))
            else:
                logger.warning('LINE 61:table:%s',table)
                df = json_normalize(list(self.db[table].find({
                    timestamp_col:
                        {
                            "$gte": start_date,
                            "$lte": end_date
                        }
                }, {'_id':False})))
                #logger.warning('LINE 68:%s',df.head())
            return df

        except Exception:
            logger.error('load df', exc_info=True)
