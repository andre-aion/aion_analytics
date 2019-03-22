from datetime import datetime, date, time, timedelta
from enum import Enum
from os.path import join, dirname
import pandas as pd
import dask as dd

from scripts.utils.mylogger import mylogger
from scripts.streaming.streamingDataframe import StreamingDataframe as SD
from scripts.storage.pythonRedis import PythonRedis
from scripts.storage.pythonParquet import PythonParquet
from scripts.storage.pythonClickhouse import PythonClickhouse
from bokeh.models.widgets import Div, Paragraph

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

    # designed to work with premade warehouse table
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
                    #logger.warning('start_date from compute:%s', params['min_date'])
                    #logger.warning('start from slider:%s', req_start_date)

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
            # no part in memory, so construct/load from clickhouse
            mintime = time(00,00,00)
            if isinstance(req_end_date,date):
                req_end_date = datetime.combine(req_end_date,mintime)
            if isinstance(req_start_date,date):
                req_start_date = datetime.combine(req_start_date,mintime)
            req_end_date = req_end_date+timedelta(days=1) #move end_date to midnite
            self.df = self.ch.load_data(self.table,self.cols,req_start_date, req_end_date)
            self.filter_df(req_start_date, req_end_date)
            #logger.warning("%s LOADED: %s:%s",self.table,req_start_date,req_end_date)

        except Exception:
            logger.error("df_load:", exc_info=True)

    def str_to_date(self, x):
        if isinstance(x, str):
            logger.warning("STR TO DATETIME CONVERSION:%s", x)
            return datetime.strptime(x, "%Y-%m-%d %H:%M:%S")
        return x

    def filter_df(self, start_date, end_date):
        self.df1 = self.df

    def divide_by_day_diff(self,x):
        y = x/self.day_diff
        logger.warning('Normalization:before:%s,after:%s',x,y)
        return y


    def normalize(self, df):
        try:
            min_date, max_date = dd.compute(df.block_timestamp.min(),
                                            df.block_timestamp.max())
            day_diff = abs((max_date - min_date).days)
            if day_diff > 0:
                for col in df.columns:
                    if isinstance(col, int) or isinstance(col, float):
                        logger.warning("NORMALIZATION ONGOING FOR %s", col)
                        df[col] = df[col].map(self.divide_by_day_diff)
            logger.warning("NORMALIZATION ended for day-diff:%s days", day_diff)
            return df
        except Exception:
            logger.error('normalize:', exc_info=True)

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
        txt = """<div style="text-align:center;background:black;width:100%;">
                <h4 style="color:#fff;">
                {}</h4></div>""".format(text)
        self.notification_div.text = txt

    def group_data(self, df, groupby_dict={}):
        # normalize columns by number of days under consideration
        df = self.normalize(df)
        df = df.groupby([self.interest_var]).agg(groupby_dict).compute()

        df = df.reset_index()
        if 'index' in df.columns.tolist():
            df = df.drop('index', axis=1)
        df = df.fillna(0)
        # logger.warning('df after groupby:%s', self.df.head(10))

        return df

    def divide_by_day_diff(self, x):
        y = x / self.day_diff
        logger.warning('Normalization:before:%s,after:%s', x, y)
        return y

    def make_filepath(self, path):
        return join(dirname(__file__), path)
    # ######################################################

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
            params = dict()
            params['in_memory'] = False
            params['key'] = None
            return params