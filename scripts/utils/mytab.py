from datetime import datetime
from scripts.utils.mylogger import mylogger
from scripts.streaming.streamingDataframe import StreamingDataframe as SD
from scripts.utils.myutils import set_params_to_load, construct_df_upon_load, \
    ms_to_date
from scripts.utils.pythonCassandra import PythonCassandra

logger = mylogger(__file__)
table = 'block'


class Mytab:
    pc = PythonCassandra()
    pc.createsession()
    pc.createkeyspace('aion')

    def __init__(self,table,cols,dedup_cols, query_cols):

        self.table = table
        self.load_params = dict()
        self.cols=cols
        self.locals = dict() # stuff local to each tab
        streaming_dataframe = SD('block', cols, dedup_cols)
        self.df = streaming_dataframe.get_df()
        self.df1 = None
        self.query_cols = query_cols
        self.dedup_cols = dedup_cols

    def load_data(self,start_date, end_date):
        end_date = datetime.combine(end_date, datetime.min.time())
        start_date = datetime.combine(start_date, datetime.min.time())


        # find the boundaries of the loaded data, redis_data
        load_params = set_params_to_load(self.df, start_date,
                                         end_date)
        logger.warning('load_data:%s', load_params)
        # load from redis, cassandra if necessary
        self.df = construct_df_upon_load(self.df,
                                         self.table,
                                         self.cols,
                                         self.dedup_cols,
                                         start_date,
                                         end_date, self.load_params)
        # filter dates
        if self.df is not None:
            if len(self.df) > 5:
                logger.warning('load_data head:%s', self.df.head())
                logger.warning('load_data tail:%s', self.df.tail())
            self.filter_df(start_date, end_date)


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
                self.df1 = self.df[self.query_cols]

        self.df1 = self.df1.set_index('block_number')

        logger.warning("post load and filter:%s",self.df1.head())



