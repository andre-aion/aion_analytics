from datetime import datetime
from scripts.utils.mylogger import mylogger
from scripts.streaming.streamingDataframe import StreamingDataframe as SD
from scripts.utils.myutils import set_params_to_load, construct_df_upon_load, \
    ms_to_date
from scripts.utils.pythonCassandra import PythonCassandra
from scripts.utils.pythonRedis import RedisStorage, LoadType

r = RedisStorage()
logger = mylogger(__file__)
table = 'block'


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

    def load_data(self,start_date, end_date):
        logger.warning("load_data,start_date:%s", start_date)
        logger.warning("load_data,end_date:%s", end_date)
        end_date = datetime.combine(end_date, datetime.min.time())
        start_date = datetime.combine(start_date, datetime.min.time())
        logger.warning("load_data,start_date:%s",start_date)
        logger.warning("load_data,end_date:%s",end_date)


        # find the boundaries of the loaded data, redis_data
        load_params = set_params_to_load(self.df, start_date,
                                         end_date)

        logger.warning('load_data:%s', load_params)
        # if table not in live memory then go to redis and cassandra
        if load_params['in_memory'] == False:
            if self.table != 'block_tx_warehouse':
                # load from redis, cassandra if necessary
                self.df = construct_df_upon_load(self.df,
                                                 self.table,
                                                 self.cols,
                                                 self.dedup_cols,
                                                 start_date,
                                                 end_date, self.load_params)
                self.filter_df(start_date, end_date)
            else:
                params = r.set_load_params(self.table, start_date, end_date, load_params)
                # LOAD ALL FROM REDIS
                if params['load_type'] & LoadType.REDIS_FULL.value == LoadType.REDIS_FULL.value:
                    lst = params['redis_key_full'].split(':')
                    sdate = r.date_to_ms(lst[1])
                    edate = r.date_to_ms(lst[2])
                    self.df = r.load(table, sdate, edate, params['redis_key_full'], 'dataframe')
                    self.filter_df(start_date, end_date)
                    return False
                    # load from source other than 100% redis
                else: # load block and tx and make the warehouse
                    return True
            # if block tx warehouse is the table under consideration
            # but it is neither in redis or in live data, then load block
            # and transaction
        else: # if table in live memory
            self.filter_df(start_date, end_date)
        return False


    def filter_df(self, start_date, end_date):
        # change from milliseconds to seconds
        start_date = ms_to_date(start_date)
        end_date = ms_to_date(end_date)

        # set df1 while outputting bar graph
        self.df1 = self.df[(self.df.block_date >= start_date) &
                           (self.df.block_date <= end_date)]

        # slice to retain cols
        if self.query_cols is not None:
            if len(self.query_cols) > 0:
                self.df1 = self.df1[self.query_cols]

        self.df1 = self.df1.reset_index()

        logger.warning("post load and filter:%s",self.df1.head())



