from datetime import datetime
from enum import Enum
from os.path import join, dirname
import pandas as pd
import dask as dd

from scripts.utils.mylogger import mylogger
from scripts.streaming.streamingDataframe import StreamingDataframe as SD
from scripts.storage.pythonRedis import PythonRedis
from scripts.storage.pythonParquet import PythonParquet
from scripts.storage.pythonClickhouse import PythonClickhouse
from scripts.utils.dashboard.poolminer import make_poolminer_warehouse
from bokeh.models.widgets import Div, Paragraph
from config import table_dict, columns

r = PythonRedis()
logger = mylogger(__file__)


class DataLocation(Enum):
    IN_MEMORY = 1
    IN_REDIS = 2
    IN_CONSTRUCTION = 4


class Mytab:
    def __init__(self, table, cols, dedup_cols):
        self.table = table
        self.load_params = dict()
        self.cols = cols
        self.locals = dict()  # stuff local to each tab
        self.streaming_dataframe = SD(table, cols, dedup_cols)
        self.df = self.streaming_dataframe.df
        self.df1 = None
        self.dedup_cols = dedup_cols
        self.params = None
        self.load_params = None
        self.poolname_dict = self.get_poolname_dict()
        self.key_tab = ''  # for key composition in redis
        self.construction_tables = {}
        self.tier1_miners_list = []
        self.tier2_miners_list = []
        self.pq = PythonParquet()
        self.ch = PythonClickhouse('aion')
        self.redis = PythonRedis()
        self.conn = self.redis.conn

        # create warehouse is needed
        if 'warehouse' in self.table:
            self.ch.create_table(self.table, table_dict, columns)

    def df_load(self, req_start_date, req_end_date):
        params = {
            'start': False,
            'end': False
        }

        try:
            if self.df is not None:
                if len(self.df) > 0:
                    # if in memory simply filter
                    params['min_date'], params['max_date'] = \
                        dd.compute(self.df.block_timestamp.min(), self.df.block_timestamp.max())

                    req_start_date = pd.to_datetime(req_start_date)
                    req_end_date = pd.to_datetime(req_end_date)

                    # check start
                    logger.warning('start_date from compute:%s', params['min_date'])
                    logger.warning('start from slider:%s', req_start_date)

                    # set flag to true if data is in memory
                    if req_start_date >= params['min_date']:
                        params['start'] = True
                    if req_end_date <= params['max_date']:
                        params['end'] = True

            # entire frame in memory
            key_params = [self.table, self.key_tab]

            if params['start'] and params['end']:
                self.filter_df(req_start_date, req_end_date)
                logger.warning("DF LOADED FROM MEMORY:%s", self.table)
            else: # no part in memory, so construct/load from clickhouse

                if 'warehouse' in self.table:
                    # check memory
                    memory_params = self.is_in_memory(self.table, req_start_date, req_end_date)
                    if memory_params['in_memory']:
                        #load from redis
                        self.df = self.redis.load(key_params,req_start_date,req_end_date,memory_params['key'])
                        logger.warning("WAREHOUSE LOADED FROM REDIS:%s")
                    else:
                        # load the construction tables df_tx and df_block
                        self.construction_tables['block'].df_load(req_start_date,
                                                                  req_end_date)
                        self.construction_tables['transaction'].df_load(req_start_date,
                                                                        req_end_date)
                        # make the warehouse
                        self.df = make_poolminer_warehouse(
                            self.construction_tables['transaction'].df,
                            self.construction_tables['block'].df,
                            req_start_date,
                            req_end_date,
                            self.key_tab)

                        #save to parquet
                        #logger.warning("WAREHOUSE TO SAVE TO REDIS:%s", self.df.head())
                        self.redis.save(self.df, key_params, req_start_date, req_end_date)

                else:  # Non warehouse
                    self.df = self.ch.load_data(self.table,self.cols,req_start_date, req_end_date)
            self.filter_df(req_start_date, req_end_date)
            logger.warning("%s LOADED: %s:%s",self.table,req_start_date,req_end_date)

        except Exception:
            logger.error("df_load:", exc_info=True)

    def str_to_date(self, x):
        if isinstance(x, str):
            logger.warning("STR TO DATETIME CONVERSION:%s", x)
            return datetime.strptime(x, "%Y-%m-%d %H:%M:%S")
        return x

    def filter_df(self, start_date, end_date):
        if len(self.df)>0:
            self.df1 = self.df.dropna(subset=['block_timestamp'])

            logger.warning("FILTER start date:%s", start_date)
            logger.warning("FILTER end date:%s", end_date)
            #logger.warning("filter before conversion: b:%s", self.df1.tail(5))

            # convert block_timestamp from str to timestamp if needed
            df = self.df1.head()
            #x = self.df1.ix[1,'block_timestamp']
            x = df['block_timestamp'].values[0]
            if isinstance(x,str):
                logger.warning("CONVERTTING BLOCK TIMESTAMP FROM STRING TO DATETIME")
                meta = ('block_timestamp', 'datetime64[ns]')
                self.df1['block_timestamp'] = self.df['block_timestamp'].map_partitions(
                    pd.to_datetime, format='%Y-%m-%d %H:%M:%S', meta=meta)


            self.df1 = self.df1[(self.df1.block_timestamp >= start_date) &
                                (self.df1.block_timestamp <= end_date)]
            #logger.warning("post filter_df:%s", self.df1['block_timestamp'].tail(5))

        else:
            self.df1 = self.streaming_dataframe.df

    def spacing_div(self, width=20, height=100):
        return Div(text='', width=width, height=height)

    def spacing_paragraph(self, width=20, height=100):
        return Paragraph(text='', width=width, height=height)

    def get_poolname_dict(self):
        file = join(dirname(__file__), '../../../data/poolinfo.csv')
        df = pd.read_csv(file)
        a = df['address'].tolist()
        b = df['poolname'].tolist()
        poolname_dict = dict(zip(a, b))
        return poolname_dict

    def poolname_verbose(self, x):
        # add verbose poolname
        if x in self.poolname_dict.keys():
            return self.poolname_dict[x]
        return x

    def poolname_verbose_trun(self, x):
        if x in self.poolname_dict.keys():
            return self.poolname_dict[x]
        else:
            if len(x) > 10:
                return x[0:10]
        return x

    def notification_updater(self, text):
        return '<h3  style="color:red">{}</h3>'.format(text)


    def is_in_memory(self,table,req_start_date,req_end_date):
        str_req_start_date = datetime.strftime(req_start_date, '%Y-%m-%d')
        str_req_end_date = datetime.strftime(req_end_date, '%Y-%m-%d')
        logger.warning('set_load_params-req_start_date:%s', str_req_start_date)
        logger.warning('set_load_params-req_end_date:%s', str_req_end_date)
        params = dict()
        params['in_memory'] = False
        params['key'] = None

        try:
            # get keys
            str_to_match = '*' + table + ':*'
            matches = self.conn.scan_iter(match=str_to_match)

            if matches:
                for redis_key in matches:
                    redis_key_encoded = redis_key
                    redis_key = str(redis_key, 'utf-8')
                    logger.warning('redis_key:%s', redis_key)

                    redis_key_list = redis_key.split(':')
                    logger.warning('matching keys :%s', redis_key_list)
                    # convert start date in key to datetime
                    key_start_date = datetime.strptime(redis_key_list[-2], '%Y-%m-%d')
                    key_end_date = datetime.strptime(redis_key_list[-1], '%Y-%m-%d')

                    # check to see if there is all data to be retrieved from reddis
                    logger.warning('req_start_date:%s', req_start_date)
                    logger.warning('key_start_date:%s', key_start_date)

                    # matches to delete: make a list
                    if req_start_date >= key_start_date and req_end_date <= key_end_date:
                        """
                        required_df      || ---------------- ||
                        redis_df  | -------------------------------|
                        """
                        params['in_memory'] = True
                        params['key'] = redis_key_encoded
                        break
            return params
        except Exception:
            logger.error("is_in_memory",exc_info=True)
            return params



