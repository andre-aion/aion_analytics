import datetime
from time import mktime

from scripts.streaming.streamingDataframe import StreamingDataframe as SD
from scripts.utils.mylogger import mylogger
from config import insert_sql, create_table_sql, create_indexes
from concurrent.futures import ThreadPoolExecutor
from clickhouse_driver import Client as Clickhouse_Client


import logging
from cassandra.cluster import Cluster, BatchStatement
import pandas as pd
import dask as dd
import numpy as np
from datetime import datetime
import gc
from pdb import set_trace

executor = ThreadPoolExecutor(max_workers=20)

logger = mylogger(__file__)



import gc
from pdb import set_trace

logger = mylogger(__file__)


class PythonClickhouse:
    # port = '9000'

    def __init__(self,db):
        self.client = Clickhouse_Client('localhost')
        self.db = db

    def create_database(self, db='aion'):
        self.db = db
        sql = 'CREATE DATABASE IF NOT EXISTS {}'.format(self.db)
        self.client.execute(sql)

    # convert dates from any timestamp to clickhouse dateteime
    def ts_to_date(self, ts, precision='s'):
        try:
            if isinstance(ts, int):
                # change milli to seconds
                if ts > 16307632000:
                    ts = ts // 1000
                if precision == 'ns':
                    ts = datetime.utcfromtimestamp(ts)
                    # convert to nanosecond representation
                    ts = np.datetime64(ts).astype(datetime)
                    ts = pd.Timestamp(datetime.date(ts))
                elif precision == 's':  # convert to ms respresentation
                    ts = datetime.fromtimestamp(ts)

            elif isinstance(ts, datetime):
                logger.warning("IS DATETIME")
                return ts
            elif isinstance(ts,str):
                logger.warning("IS STRING")
                return datetime.strptime(ts,"%Y-%m-%d %H:%M:%S")

            logger.warning('ts_to_date: %s', ts)
            return ts
        except Exception:
            logger.error('ms_to_date', exc_info=True)
            return ts

    def create_table(self, table, table_dict, columns):
        try:
            qry = self.construct_create_query(table, table_dict, columns)
            self.client.execute(qry)
            logger.warning('{} SUCCESSFULLY CREATED:%s', table)
        except Exception:
            logger.error("Create table error", exc_info=True)

    def construct_read_query(self, table, cols, startdate, enddate):
        qry = 'SELECT '
        if len(cols) >= 1:
            for pos, col in enumerate(cols):
                if pos > 0:
                    qry += ','
                qry += col
        else:
            qry += '*'

        qry += """ FROM {}.{} WHERE toDate(block_timestamp) >= toDate('{}') AND 
               toDate(block_timestamp) <= toDate('{}') ORDER BY block_timestamp""" \
            .format(self.db,table, startdate, enddate)

        logger.warning('query:%s', qry)
        return qry

    def load_data(self,table,cols,start_date,end_date):
        start_date = self.ts_to_date(start_date)
        end_date = self.ts_to_date(end_date)
        logger.warning('load data start_date:%s', start_date)
        logger.warning('load_data  to_date:%s', end_date)

        if start_date > end_date:
            logger.warning("END DATE IS GREATER THAN START DATE")
            logger.warning("BOTH DATES SET TO START DATE")
            start_date = end_date
        sql = self.construct_read_query(table, cols, start_date, end_date)

        try:
            query_result = self.client.execute(sql, settings={'max_execution_time': 3600})
            df = pd.DataFrame(query_result, columns=cols)
            df = dd.dataframe.from_pandas(df, npartitions=15)
            #sd = SD(table,cols,[])
            #sd.df = dd.
            #logger.warning("query result:%s",df.tail(5))
            return df

        except Exception:
            logger.error(' load data :%s', exc_info=True)

