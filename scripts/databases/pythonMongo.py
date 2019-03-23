from pymongo import MongoClient
from scripts.utils.mylogger import mylogger

import pandas as pd
import dask as dd
import numpy as np
from datetime import datetime

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
