from datetime import datetime, timedelta, date

import pydot
from bokeh.layouts import gridplot
from bokeh.models import Panel, Div, DatePicker, WidgetBox, Button, Select
from sklearn import metrics
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix, classification_report
from sklearn.model_selection import train_test_split
from sklearn.tree import export_graphviz
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline

from scripts.databases.pythonClickhouse import PythonClickhouse
from scripts.utils.dashboards.EDA.mytab_interface import Mytab
from scripts.utils.mylogger import mylogger
from scripts.utils.myutils import datetime_to_date
from scripts.streaming.streamingDataframe import StreamingDataframe as SD
from config.dashboard import config as dashboard_config

from tornado.gen import coroutine

from operator import itemgetter
import pandas as pd
import dask as dd
import holoviews as hv
from holoviews import streams

from scripts.utils.myutils import tab_error_flag
from config.hyp_variables import groupby_dict
logger = mylogger(__file__)

hv.extension('bokeh', logo=False)
renderer = hv.renderer('bokeh')

table = 'crypto_modelling'
hyp_variables= list(groupby_dict[table].keys())
groupby_dict = groupby_dict[table]


@coroutine
def cryptocurrency_tab(cryptos):
    class Thistab(Mytab):
        def __init__(self, table, cols,cryptos,dedup_cols=[]):
            Mytab.__init__(self, table, cols, dedup_cols)
            self.table = table
            self.cols = cols
            self.DATEFORMAT = "%Y-%m-%d %H:%M:%S"
            self.df = None
            self.df1 = {}  # to contain churned and retained splits
            self.df_predict = None
            self.day_diff = 1  # for normalizing for classification periods of different lengths
            self.df_grouped = ''

            self.cl = PythonClickhouse('aion')
            self.feature_list = hyp_variables
            self.items = cryptos
            # add all the coins to the dict
            self.github_cols = ['watch','fork','issue','release','push']
            self.index_cols = ['close','high','low','market_cap','volume']
            self.groupby_dict = self.set_groupby_dict(groupby_dict)
            self.vars_dict = {
                'watch':[],
                'fork':[],
                'issue':[],
                'release':[],
                'push':[],
                'close':[],
                'high':[],
                'low':[],
                'market_cap':[],
                'volume':[]
            }

        """
            Melt coin data into composite columns
        """
        def melt_df(self,df):
            try:
                # convert from dask to pandas
                df = df.compute()
                df = df.melt(id_vars=['date'])
                logger.warning('df after melt:%s',df.head(50))
                '''
                # melt dataframe
                for key,value in self.vars_dict.items():
                    value = list(set(value)) # ensure uniqueness in list
                '''
                return df

            except Exception:
                logger.error('melt coins',exc_info=True)

        """
            groupby day of year
        """

        def set_groupby_dict(self,groupby_dict):
            try:
                for crypto in cryptos:
                    for col in self.github_cols:
                        key = crypto+'_'+col
                        self.vars_dict[col].append(key)
                        if key not in groupby_dict.keys():
                            groupby_dict[key] = 'mean'
                    for col in self.index_cols:
                        key = crypto+'_'+col
                        self.vars_dict[col].append(key)
                        if key not in groupby_dict.keys():
                            groupby_dict[key] = 'mean'
                self.groupby_dict = groupby_dict
            except Exception:
                logger.error('set groupby dict',exc_info=True)

        def prep_data(self,df):
            try:
                # groupby
                df = df.assign(date=datetime(df['timestamp'].dt.date))
                cols = list(df.groupby_dict.keys()).append('date')
                df = df[cols]
                df = df.grouby(['date']).agg(self.grouby_dict)

                # melt
                df = self.melt_df(df)

            except Exception:
                logger.error('prep data', exc_info=True)


# SETUP
    table = 'account_ext_warehouse'
    #cols = list(table_dict[table].keys())

    cols = hyp_variables + ['timestamp']
    thistab = Thistab(table, cols,cryptos,[])

    # setup dates
    first_date_range = datetime.strptime("2018-04-25 00:00:00", "%Y-%m-%d %H:%M:%S")
    last_date_range = datetime.now().date()
    last_date = dashboard_config['dates']['last_date']
    first_date = last_date - timedelta(days=2)

    thistab.df_load(first_date, last_date,timestamp_col='timestamp')
