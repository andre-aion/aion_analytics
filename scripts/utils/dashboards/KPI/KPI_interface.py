import gc
from math import inf

import pandas as pd
import numpy as np
from dask import dataframe as dd
from datetime import datetime,timedelta,date
from dateutil.relativedelta import relativedelta

from scripts.databases.pythonClickhouse import PythonClickhouse
from scripts.utils.mylogger import mylogger
from scripts.utils.myutils import concat_dfs

import hvplot.pandas
import hvplot.dask

logger = mylogger(__file__)


# get rid of tzinfo
def clean_dates_from_db(x):
    try:
        return datetime(x.year, x.month, x.day, x.hour, 0, 0)
    except Exception:
        logger.error('clean dates from db', exc_info=True)


class KPI:
    menus = {
        'account_type': ['all', 'contract', 'miner', 'native_user', 'token_user'],
        'update_type': ['all', 'contract_deployment', 'internal_transfer', 'mined_block', 'token_transfer',
                        'transaction'],
        'period': ['D', 'W', 'M', 'H']
    }
    def __init__(self,table,cols):
        self.df = None
        self.ch = PythonClickhouse('aion')
        self.table = table
        self.cols = cols
        self.div_style = """ style='width:350px; margin-left:25px;
                                border:1px solid #ddd;border-radius:3px;background:#efefef50;' 
                                """

        self.header_style = """ style='color:blue;text-align:center;' """
        self.welcome_txt = """<div style="text-align:center;background:black;width:100%;">
                                         <h1 style="color:#fff;">
                                         {}</h1></div>""".format('Welcome')
        self.KPI_css = {
            'circle': """style='text-align : center; padding : 1em; border : 0.5em solid black;
                         width : 12em; height : 4em; border-radius: 100%;' """
        }
        self.DATEFORMAT = '%Y-%m-%d %H:%M:%S'
        self.initial_date = datetime.strptime("2018-04-25 00:00:00",self.DATEFORMAT)
        self.account_type = 'all'
        self.trigger = -1


        # make block timestamp the index
    def load_df(self,start_date,end_date,cols,select_col='timestamp_of_first_event'):
        try:

            if isinstance(end_date,date):
                end_date = datetime.combine(end_date,datetime.min.time())
            if isinstance(start_date,date):
                start_date = datetime.combine(start_date,datetime.min.time())

            return self.ch.load_data(self.table, cols, start_date, end_date,select_col)

            #df[select_col] = df[select_col].map(lambda x: clean_dates_from_db(x))
        except Exception:
            logger.error('load df',exc_info=True)

    def period_to_date(self,df,timestamp = None,filter_col=None,cols=[],period='week'):
        try:
            if timestamp is None:
                timestamp = datetime.now()
                timestamp = datetime(timestamp.year,timestamp.month,timestamp.day,timestamp.hour,0,0)
            if period == 'week':
                start = timestamp - timedelta(days=timestamp.weekday())
            elif period == 'month':
                start = timestamp - timedelta(days=timestamp.day)
            elif period == 'year':
                start = datetime(timestamp.year,1,1,0,0,0)
            elif period == 'quarter':
                curr_quarter = int((timestamp.month - 1) / 3 + 1)
                start = datetime(timestamp.year, 3 * curr_quarter - 2, 1)

            # filter
            if filter_col is None:
                filter_col = 'block_timestamp'
            df = df[(df[filter_col] >= start) & (df[filter_col] <= timestamp)]
            logger.warning('df length after filter:%s',len(df))
            if len(cols) >0:
                df = df[cols]
            return df
        except Exception:
            logger.error('period to date',exc_info=True)

    def shift_period_range(self,period,start,end):
        try:
            if period == 'week':
                start = start - timedelta(days=7)
                end = end - timedelta(days=7)
            elif period == 'month':
                start = start - relativedelta(months=1)
                end = end - relativedelta(months=1)
            elif period == 'year':
                start = start - relativedelta(years=1)
                end = end - relativedelta(years=1)
            elif period == 'quarter':
                start = start - relativedelta(months=3)
                end = end - relativedelta(months=3)
            return start, end
        except Exception:
            logger.error('calc start period',exc_info=True)

    def period_over_period(self,df,period,history_periods=1,timestamp_col='timestamp_of_first_event'):
        try:
            # filter cols if necessary
            string = 'current '.format(period)
            df_current = df.assign(period=string)
            logger.warning('df_current%s',df_current.head(10))
            start,end = dd.compute(df[timestamp_col].min(),df[timestamp_col].max())
            start = datetime(start.year,start.month,start.day,0,0,0)
            end = datetime(end.year,end.month, end.day,0,0,0)

            cols = list(df.columns)
            # load
            counter = 0
            while counter < history_periods and start >= self.initial_date:
                counter += 1
                start, end = self.shift_period_range(period,start,end)
                # load data
                logger.warning('start:end %s:%s', start, end)
                df_temp = self.load_df(start,end,cols)
                if df_temp is not None:
                    if len(df_temp) > 1:
                        string = '{} {} prev'.format(counter, period)
                        # label period
                        df_temp = df_temp.assign(period=string)
                        logger.warning('df temp loaded for %s previous: %s',counter,len(df_temp))

                        df_current = concat_dfs(df_current,df_temp)
                        logger.warning('df_current after concat: %s',df_current.head())
            return df_current
        except Exception:
            logger.error('period over period to date',exc_info=True)

    def notification_updater(self, text):
        txt = """<div style="text-align:center;background:black;width:100%;">
                <h4 style="color:#fff;">
                {}</h4></div>""".format(text)
        self.notification_div.text = txt


    def clean_data(self, df):
        df = df.fillna(0)
        df[df == -inf] = 0
        df[df == inf] = 0
        return df

