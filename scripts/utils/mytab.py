from datetime import datetime
from enum import Enum

from tornado.gen import coroutine

from scripts.utils.mylogger import mylogger
from scripts.streaming.streamingDataframe import StreamingDataframe as SD
from scripts.utils.myutils import set_params_to_load, construct_df_upon_load, \
    ms_to_date, date_to_ms
from scripts.utils.pythonCassandra import PythonCassandra
from scripts.utils.pythonRedis import RedisStorage, LoadType
from scripts.utils.poolminer import make_poolminer_warehouse
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

    def __init__(self,table,cols,dedup_cols, query_cols=[]):
        self.table = table
        self.load_params = dict()
        self.cols=cols
        self.locals = dict() # stuff local to each tab
        streaming_dataframe = SD(table, cols, dedup_cols)
        self.df = streaming_dataframe.get_df()
        self.df1 = None
        self.query_cols = query_cols
        self.dedup_cols = dedup_cols
        self.params = None
        self.load_params = None


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
        end_date = datetime.combine(end_date, datetime.min.time())
        start_date = datetime.combine(start_date, datetime.min.time())
        # if table not in live memory then go to redis and cassandra
        load_params = set_params_to_load(self.df, start_date,end_date)
        if load_params['in_memory'] == False:
            if self.table != 'block_tx_warehouse':
                # load from redis, cassandra if necessary
                self.df = construct_df_upon_load(self.df,
                                                 self.table,
                                                 self.cols,
                                                 self.dedup_cols,
                                                 start_date,
                                                 end_date, self.load_params)
            else: # if table is block_tx_warehouse
                # LOAD ALL FROM REDIS
                if self.params['load_type'] & LoadType.REDIS_FULL.value == LoadType.REDIS_FULL.value:
                    lst = self.params['redis_key_full'].split(':')
                    sdate = date_to_ms(lst[1])
                    edate = date_to_ms(lst[2])
                    self.df = r.load(table, sdate, edate, self.params['redis_key_full'], 'dataframe')
                    # load from source other than 100% redis
                else: # load block and tx and make the warehouse,
                    self.df = make_poolminer_warehouse(
                        df_tx,
                        df_block,
                        start_date,
                        end_date)
                    logger.warning("WAREHOUSE UPDATED WITH END DATE:%s",end_date)

    def filter_df(self, start_date, end_date):
        # change from milliseconds to seconds
        start_date = ms_to_date(start_date)
        end_date = ms_to_date(end_date)

        # set df1 while outputting bar graph
        self.df1 = self.df[(self.df.block_date >= start_date) &
                           (self.df.block_date <= end_date)]

        # slice to retain cols
        logger.warning("in filter_df:%s",self.df1.columns.tolist())
        #if self.query_cols is not None:
            #if len(self.query_cols) > 0:
                #self.df1 = self.df1[self.query_cols]
        #self.df1 = self.df1.reset_index()
        #self.df1 = self.df1.fillna(0)

        #logger.warning("post filter:%s",self.df1.head(20))
        return self.df1

