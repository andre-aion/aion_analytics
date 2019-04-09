import datetime

from tornado.gen import coroutine

from config.df_construct import columns as cols
from scripts.utils.mylogger import mylogger
from concurrent.futures import ThreadPoolExecutor
from clickhouse_driver import Client as Clickhouse_Client

import pandas as pd
import dask as dd
import numpy as np
from datetime import datetime
import sqlalchemy as sa
import pandahouse

executor = ThreadPoolExecutor(max_workers=20)
logger = mylogger(__file__)

class PythonClickhouse:
    # port = '9000'
    ch = sa.create_engine('clickhouse://default:@127.0.0.1:8123/aion')
    def __init__(self,db):
        self.client = Clickhouse_Client('localhost')
        self.db = db
        self.conn = {'host':'http://127.0.0.1:8123','database':'aion'}


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

    # cols is a dict, key is colname, type is col type
    def construct_create_query(self, table, table_dict, columns):
        logger.warning('table_dict:%s', table_dict)

        count = 0
        try:
            qry = 'CREATE TABLE IF NOT EXISTS ' + self.db + '.' + table + ' ('
            if len(table_dict) >= 1:
                for key in columns:
                    if count > 0:
                        qry += ','
                    if table == 'block':
                        logger.warning('%s:%s', key, table_dict[key])
                    qry += key + ' ' + table_dict[key]
                    count += 1
            qry += ") ENGINE = MergeTree()"

            logger.warning('create table query:%s', qry)
            return qry
        except Exception:
            logger.error("Construct table query",exc_info=True)

    def create_table(self, table, table_dict, columns):
        try:
            qry = self.construct_create_query(table, table_dict, columns)
            self.client.execute(qry)
            logger.warning('{} SUCCESSFULLY CREATED:%s', table)
        except Exception:
            logger.warning('%s NOT CREATED',table.upper())
            logger.error("Create table error", exc_info=True)

    def construct_read_query(self, table, cols, startdate, enddate,timestamp_col='block_timestamp'):
        qry = 'SELECT '

        if len(cols) >= 1:
            for pos, col in enumerate(cols):
                if pos > 0:
                    qry += ','
                qry += col
        else:
            qry += '*'

        qry += """ FROM {}.{} WHERE toDate({}) >= toDate('{}') AND 
               toDate({}) <= toDate('{}') ORDER BY {}""" \
            .format(self.db,table,timestamp_col,startdate,timestamp_col, enddate,timestamp_col)

        #logger.warning('query:%s', qry)
        return qry

    def load_data(self,table,cols,start_date,end_date,timestamp_col='block_timestamp'):
        start_date = self.ts_to_date(start_date)
        end_date = self.ts_to_date(end_date)
        #logger.warning('load data start_date:%s', start_date)
        #logger.warning('load_data  to_date:%s', end_date)

        if start_date > end_date:
            logger.warning("END DATE IS GREATER THAN START DATE")
            logger.warning("BOTH DATES SET TO START DATE")
            start_date = end_date
        sql = self.construct_read_query(table, cols, start_date, end_date,timestamp_col)

        try:
            query_result = self.client.execute(sql,settings={'max_execution_time': 3600})
            df = pd.DataFrame(query_result, columns=cols)
            # if transaction table change the name of nrg_consumed
            if table in ['transaction', 'block']:
                if 'nrg_consumed' in df.columns.tolist():
                    new_name = table + '_nrg_consumed'
                    df = df.rename(index=str, columns={"nrg_consumed": new_name})
                    #new_columns = [new_name if x == 'nrg_consumed' else x for x in df.columns.tolist()]
                    #logger.warning("columns renamed:%s", df.columns.tolist())
            df = dd.dataframe.from_pandas(df, npartitions=15)
            #logger.warning('columns loaded:%s',df.columns.tolist())
            #logger.warning("DATA LOADED:%s", df.head(10))


            return df

        except Exception:
            logger.error(' load data :%s', exc_info=True)

        # cols is a dict, key is colname, type is col type

    def construct_create_query(self, table, table_dict, columns):

        count = 0
        try:
            qry = 'CREATE TABLE IF NOT EXISTS ' + self.db + '.' + table + ' ('
            if len(table_dict) >= 1:
                for key in columns:
                    if count > 0:
                        qry += ','
                    qry += key + ' ' + table_dict[key]
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
        qry = 'INSERT INTO ' + self.db + '.' + table + ' ' + cols + ' VALUES '
        for idx, message in enumerate(messages):
            if idx > 0:
                qry += ','
            qry += message
        qry += "'"
        #logger.warning('data insert query:%s', qry)
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
        try:
            logger.warning("INSIDE SAVE PANDAS DF")
            #df.to_sql(table,self.ch,if_exists='append',index=False)
            logger.warning("%s inserted to clickhouse",table.upper())

            messages = list(zip(df['block_number'],df['block_timestamp'],df['transaction_hash'],df['miner_address'],
            df['total_difficulty'],df['difficulty'],
            df['block_nrg_consumed'],df['nrg_limit'],df['num_transactions'],
            df['block_size'],df['block_time'],df['nrg_reward'],df['block_year'],df['block_month'],
            df['block_day'],df['from_addr'],
            df['to_addr'],df['value'],df['transaction_nrg_consumed'],df['nrg_price']))

            self.insert(table,cols[table],messages)
            logger.warning("AFTER SAVE PANDAS DF")

        except Exception:
            logger.error("Save df", exc_info=True)

    def delete_data(self,start_range, end_range,
                    table,db='aion',col='block_timestamp'):
        DATEFORMAT = "%Y-%m-%d %H:%M:%S"
        if not isinstance(start_range,str):
            start_range = datetime.strftime(start_range,DATEFORMAT)
        if not isinstance(end_range, str):
            end_range = datetime.strftime(end_range,DATEFORMAT)
        try:
            if col == 'block_timestamp':
                qry = """ALTER TABLE {}.{} DELETE WHERE toDate({}) >= toDate('{}') and 
                    toDate({}) <= toDate('{}')
                """.format(db,table,col,start_range,col,end_range)
            else:
                qry = """ALTER TABLE {}.{} DELETE WHERE {} >= {} and 
                                    {} <= {}
                                """.format(db, table, col, start_range, col, end_range)
            #logger.warning("DELETE QRY:%s",qry)
            self.client.execute(qry)
            logger.warning("SUCCESSFUL DELETE OVER RANGE %s:%s",start_range,end_range)
        except Exception:
            logger.error("Delete_data", exc_info=True)

    def get_min_max(self,table,col):
        qry = "SELECT min({}), max({}) FROM aion.{}".format(col,col,table)

    def insert_df(self,df,table):
        try:
            df = df[cols[table]]  # arrange order of columns for
            logger.warning("columns in df to insert:%s",df.columns.tolist())
            affected_rows = pandahouse.to_clickhouse(df, table=table, connection=self.conn, index=False)
            logger.warning("DF UPSERTED:%s", affected_rows)
        except:
            logger.error("insert_df", exc_info=True)

    @coroutine
    def upsert_df(self,df,table='',col='block_timestamp'):
        try:
            df = df.compute()
            """
            - get min max of range to use as start and end of range
            - delete data
            - insert data
            """
            start_range = df[col].min()
            end_range = df[col].max()
            self.delete_data(start_range,end_range,table)
            self.insert_df(df,table)
        except Exception:
            logger.error("Upsert df", exc_info=True)


