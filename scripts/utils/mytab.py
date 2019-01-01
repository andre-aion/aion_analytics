from datetime import datetime
from enum import Enum
from os.path import join, dirname
import pandas as pd

from tornado.gen import coroutine

from scripts.utils.mylogger import mylogger
from scripts.streaming.streamingDataframe import StreamingDataframe as SD
from scripts.utils.myutils import set_params_to_load, construct_df_upon_load, \
    ms_to_date, date_to_ms
from scripts.utils.pythonCassandra import PythonCassandra
from scripts.utils.pythonRedis import RedisStorage, LoadType
from scripts.utils.poolminer import make_poolminer_warehouse
from bokeh.models.widgets import Div, Paragraph

r = RedisStorage()
logger = mylogger(__file__)
table = 'block'

class DataLocation(Enum):
    IN_MEMORY = 1
    IN_REDIS = 2
    IN_CONSTRUCTION = 4


class Mytab:
    pc = PythonCassandra()
    pc.createsession()
    pc.createkeyspace('aion')

    def __init__(self,table,cols,dedup_cols):
        self.table = table
        self.load_params = dict()
        self.cols=cols
        self.locals = dict() # stuff local to each tab
        streaming_dataframe = SD(table, cols, dedup_cols)
        self.df = streaming_dataframe.get_df()
        self.df1 = None
        self.dedup_cols = dedup_cols
        self.params = None
        self.load_params = None
        self.poolname_dict = self.get_poolname_dict()
        self.key_tab = ''  # for key composition in redis


    def is_data_in_memory(self,start_date,end_date):
        end_date = datetime.combine(end_date, datetime.min.time())
        start_date = datetime.combine(start_date, datetime.min.time())
        # find the boundaries of the loaded data, redis_data
        load_params = set_params_to_load(self.df, start_date,
                                         end_date)
        self.load_params = load_params
        logger.warning('is_data_in_memory:%s', self.load_params)

        # if table not in live memory then go to redis and cassandra
        if load_params['in_memory'] == False:
            self.params = r.set_load_params(self.table, start_date, end_date,
                                            self.load_params)
            if self.table != 'block_tx_warehouse':
                return DataLocation.IN_CONSTRUCTION
            else:  # if table is block_tx_warehouse
                # LOAD ALL FROM REDIS
                if self.params['load_type'] & LoadType.REDIS_FULL.value == \
                        LoadType.REDIS_FULL.value:
                    return DataLocation.IN_REDIS
                else:  # load block and tx and make the warehouse
                    return DataLocation.IN_CONSTRUCTION
        else:  # if table in live memory
            return DataLocation.IN_MEMORY

        return DataLocation.IN_CONSTRUCTION

    def load_data(self,start_date, end_date,df_tx=None,df_block=None):
        try:
            end_date = datetime.combine(end_date, datetime.min.time())
            start_date = datetime.combine(start_date, datetime.min.time())
            # if table not in live memory then go to redis and cassandra
            load_params = set_params_to_load(self.df, start_date,end_date)
            logger.warning("TABLE:%s",self.table)
            self.params = r.set_load_params(self.table, start_date, end_date,
                                            load_params)
            if load_params['in_memory'] == False:
                if self.table != 'block_tx_warehouse':
                    # load from redis, cassandra if necessary
                    self.df = construct_df_upon_load(self.df,
                                                     self.table,
                                                     self.key_tab,
                                                     self.cols,
                                                     self.dedup_cols,
                                                     start_date,
                                                     end_date, self.load_params,
                                                     cass_or_ch='clickhouse')
                else: # if table is block_tx_warehouse
                    # LOAD ALL FROM REDIS
                    if self.params['load_type'] & LoadType.REDIS_FULL.value == LoadType.REDIS_FULL.value:
                        lst = self.params['redis_key_full'].split(':')
                        sdate = date_to_ms(lst[1])
                        edate = date_to_ms(lst[2])
                        key_params = ['block_tx_warehouse']
                        self.df = r.load(key_params, sdate, edate, self.params['redis_key_full'], 'dataframe')
                        # load from source other than 100% redis
                    else: # load block and tx and make the warehouse,
                        self.df = make_poolminer_warehouse(
                            df_tx,
                            df_block,
                            start_date,
                            end_date)
                        logger.warning("WAREHOUSE UPDATED WITH END DATE:%s",end_date)
        except Exception:
            logger.error("load_data:",exc_info=True)

    def filter_df(self, start_date, end_date):
        # change from milliseconds to seconds
        start_date = ms_to_date(start_date)
        end_date = ms_to_date(end_date)

        # set df1 while outputting bar graph
        self.df1 = self.df[(self.df.block_date >= start_date) &
                           (self.df.block_date <= end_date)]

        # slice to retain cols
        logger.warning("in filter_df:%s",self.df1.columns.tolist())

        #self.df1 = self.df1.reset_index()
        #self.df1 = self.df1.fillna(0)


        #logger.warning("post filter:%s",self.df1.head(20))

    def spacing_div(self, width=20, height=100):
        return Div(text='', width=width, height=height)

    def spacing_paragraph(self,width=20, height=100):
        return Paragraph(text='', width=width, height=height)

    def get_poolname_dict(self):
        file = join(dirname(__file__), '../../data/poolinfo.csv')
        df = pd.read_csv(file)
        a = df['address'].tolist()
        b = df['poolname'].tolist()
        poolname_dict = dict(zip(a, b))
        return poolname_dict

    def poolname_verbose(self,x):
        # add verbose poolname
        if x in self.poolname_dict.keys():
            return self.poolname_dict[x]
        return x

    def poolname_verbose_trun(self,x):
        if x in self.poolname_dict.keys():
            return self.poolname_dict[x]
        else:
            if len(x) > 10:
                return x[0:10]
        return x

    def notification_updater(self,text):
        return '<h3  style="color:red">{}</h3>'.format(text)


