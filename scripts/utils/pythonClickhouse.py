import datetime
from time import mktime

from scripts.utils.mylogger import mylogger
from config import insert_sql, create_table_sql, create_indexes
from concurrent.futures import ThreadPoolExecutor
from clickhouse_driver import Client as Clickhouse_Client


import logging
from cassandra.cluster import Cluster, BatchStatement
import pandas as pd
import dask as dd
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


    def create_table(self, table, table_dict, columns):
        try:
            qry = self.construct_create_query(table, table_dict, columns)
            self.client.execute(qry)
            logger.warning('{} SUCCESSFULLY CREATED:%s', table)
        except Exception:
            logger.error("Create table error", exc_info=True)

    def construct_read_query(self, table, cols, startdate, enddate):
        qry = 'select '
        if len(cols) >= 1:
            for pos, col in enumerate(cols[table]):
                if pos > 0:
                    qry += ','
                qry += col
        else:
            qry += '*'

        qry += """ from {} where block_date >={} and 
               block_date <={}""" \
            .format(table, startdate, enddate)

        logger.warning('query:%s', qry)
        return qry

    def load_data(self,table,cols,start_date,end_date):
        logger.warning('cass load from_date:%s', start_date)
        logger.warning('cass load to_date:%s', end_date)
        if start_date > end_date:
            logger.warning("END DATE IS GREATER THAN START DATE")
            logger.warning("BOTH DATES SET TO START DATE")
            start_date = end_date
        sql = self.construct_read_query(table, cols, start_date, end_date)

        try:
            query_result = self.client.execute(sql, settings={'max_execution_time': 3600})
            df = pd.DataFrame(query_result, columns=cols)
            df = dd.dataframe.from_pandas(df, npartitions=15)
        except Exception:
            logger.error(' load data :%s', exc_info=True)

