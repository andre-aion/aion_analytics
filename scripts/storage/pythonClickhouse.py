import datetime

from tornado.gen import coroutine

from scripts.utils.mylogger import mylogger
from concurrent.futures import ThreadPoolExecutor
from clickhouse_driver import Client as Clickhouse_Client

import pandas as pd
import dask as dd
import numpy as np
from datetime import datetime
import sqlalchemy as sa

executor = ThreadPoolExecutor(max_workers=20)
logger = mylogger(__file__)

logger = mylogger(__file__)

class PythonClickhouse:
    # port = '9000'
    ch = sa.create_engine('clickhouse://default:@127.0.0.1:8123/aion')
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
                return ts
            elif isinstance(ts,str):
                return datetime.strptime(ts,"%Y-%m-%d %H:%M:%S")

            #logger.warning('ts_to_date: %s', ts)
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
        #logger.warning('load data start_date:%s', start_date)
        #logger.warning('load_data  to_date:%s', end_date)

        if start_date > end_date:
            logger.warning("END DATE IS GREATER THAN START DATE")
            logger.warning("BOTH DATES SET TO START DATE")
            start_date = end_date
        sql = self.construct_read_query(table, cols, start_date, end_date)

        try:
            query_result = self.client.execute(sql,
                                               settings={
                                               'max_execution_time': 3600})
            df = pd.DataFrame(query_result, columns=cols)
            # if transaction table change the name of nrg_consumed
            if table in ['transaction', 'block']:
                if 'nrg_consumed' in df.columns.tolist():
                    new_name = table + '_nrg_consumed'
                    df = df.rename(index=str, columns={"nrg_consumed": new_name})
                    #new_columns = [new_name if x == 'nrg_consumed' else x for x in df.columns.tolist()]
                    #df = df.rename(columns=dict(zip(df.columns.tolist(), new_columns)))
                    logger.warning("columns renamed:%s", df.columns.tolist())
            df = dd.dataframe.from_pandas(df, npartitions=15)

            return df

        except Exception:
            logger.error(' load data :%s', exc_info=True)

        # cols is a dict, key is colname, type is col type

    def construct_create_query(self, table, table_dict, columns):
        logger.warning('table_dict:%s', table_dict)

        count = 0
        try:
            qry = 'CREATE TABLE IF NOT EXISTS ' + self.db + '.' + table + ' ('
            if len(table_dict) >= 1:
                for key in columns[table]:
                    if count > 0:
                        qry += ','
                    if table == 'block':
                        logger.warning('%s:%s', key, table_dict[table][key])
                    qry += key + ' ' + table_dict[table][key]
                    count += 1
            qry += ") ENGINE = MergeTree() ORDER BY (block_timestamp)"

            logger.warning('create table query:%s', qry)
            return qry
        except Exception:
            logger.error("Construct table query")

    def create_table(self, table, table_dict, columns):
        try:
            qry = self.construct_create_query(table, table_dict, columns)
            self.client.execute(qry)
            logger.warning('{} SUCCESSFULLY CREATED:%s', table)
        except Exception:
            logger.error("Create table error", exc_info=True)

    def construct_insert_query(self, table, cols, messages):
        # messages is list of tuples similar to cassandra
        qry = 'INSERT INTO ' + self.db + '.' + self.table + ' ' + cols[table] + ' VALUES '
        for idx, message in enumerate(messages):
            if idx > 0:
                qry += ','
            qry += message
        qry += "'"
        logger.warning('data insert query:%s', qry)
        return qry

    def insert(self, table, cols, messages):
        qry = self.construct_insert_query(table, cols, messages)
        try:
            self.client.execute(qry)
            logger.warning('DATA SUCCESSFULLY INSERTED TO {}:%s', qry, table)
        except Exception:
            logger.error("Create table error", exc_info=True)

    def delete(self, item, type="table"):
        if type == 'table':
            self.client.execute("DROP TABLE IF EXISTS {}".format(item))
        logger.warning("%s deleted from clickhouse", item)

    def save_pandas_df(self,df,table='block_tx_warehouse'):
        df.to_sql(table,self.ch,if_exists='append',index=False)
        logger.warning("%s inserted to clickhouse",table.upper())

    def get_min_max(self,table,col):
        qry = "SELECT min({}), max({}) FROM aion.{}".format(col,col,table)

    @coroutine
    def save_df(self,df_to_save):
        df_to_save.map_partitions(lambda df: self.save_pandas_df)

