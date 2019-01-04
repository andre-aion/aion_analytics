from datetime import datetime
from enum import Enum
from os.path import join, dirname
import pandas as pd
import dask as dd

from scripts.utils.mylogger import mylogger
from scripts.streaming.streamingDataframe import StreamingDataframe as SD
from scripts.utils.myutils import set_params_to_load, construct_df_upon_load, \
    ms_to_date, date_to_ms, set_construct_params, LoadType, construct_warehouse_from_parquet_and_df, drop_cols
from scripts.storage.pythonRedis import RedisStorage
from scripts.storage.pythonParquet import PythonParquet
from scripts.storage.pythonClickhouse import PythonClickhouse
from scripts.utils.poolminer import make_poolminer_warehouse
from bokeh.models.widgets import Div, Paragraph
from config import table_dict, columns
import pandahouse

r = RedisStorage()
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
            if 'warehouse' in self.table:
                key_params = [self.table]
            else:
                key_params = [self.table, self.key_tab]

            if params['start'] and params['end']:
                self.filter_df(req_start_date, req_end_date)
                logger.warning("DF LOADED FROM MEMORY:%s", self.table)

            else: # no part in memory, so construct/load from clickhouse
                if 'warehouse' in self.table:
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
                        req_end_date)

                    #save to parquet
                    #self.pq.save(self.df, key_params, req_start_date, req_end_date)
                    logger.warning("WAREHOUSE CONSTRUCTED FROM CLICKHOUSE:%s", self.table)
                    self.df = self.df.dropna(subset=['block_date', 'block_timestamp'])
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
        self.df1 = self.df
        logger.warning("post filter:%s", self.df1.tail(10))

        '''
        # change from milliseconds to seconds
        start_date = ms_to_date(start_date)
        end_date = ms_to_date(end_date)

        cols_to_drop = ['index']
        self.df = drop_cols(self.df, cols_to_drop)

        self.df = self.df.dropna(subset=['block_date', 'block_timestamp'])

        logger.warning("in filter_df block_date:%s", self.df['block_date'].tail(5))

        # convert date to and filter on block_date
        self.df1 = self.df
        logger.warning("TABLE BEING FILTERED:%s", self.table.upper())

        logger.warning("FILTER start date:%s", start_date)
        logger.warning("FILTER end date:%s", end_date)

        meta = ('block_date', 'datetime64[ns]')
        self.df1['block_date'] = self.df1['block_date'].map_partitions(
            pd.to_datetime, format='%Y-%m-%d %H:%M:%S', meta=meta)

        logger.warning("in filter_df block_date, after conversion to datetime:%s",
                       self.df1['block_date'].tail(5))

        self.df1 = self.df1[(self.df1.block_date >= start_date) &
                            (self.df1.block_date <= end_date)]

        '''

    def spacing_div(self, width=20, height=100):
        return Div(text='', width=width, height=height)

    def spacing_paragraph(self, width=20, height=100):
        return Paragraph(text='', width=width, height=height)

    def get_poolname_dict(self):
        file = join(dirname(__file__), '../../data/poolinfo.csv')
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




