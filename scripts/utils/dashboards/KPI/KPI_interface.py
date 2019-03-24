import gc
from math import inf
from os.path import join, dirname

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
        'history_periods': ['1','2','3','4','5','6','7','8','9','10']
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
        css_path = join(dirname(__file__),"../../../static/css/KPI_interface.css")
        self.KPI_card_css = {
            'circle': """style='text-align : center; padding : 1em; border : 0.5em solid black;
                         width : 12em; height : 4em; border-radius: 100%;' """,
            'folders': """ style = 'padding : 1em;
                          width: 100%;
                          height: 200px;
                          box-sizing: border-box;
                          background-color: #E4E4D9;
                          background-image: linear-gradient(175deg, rgba(0,0,0,0) 95%, #8da389 95%),
                                            linear-gradient( 85deg, rgba(0,0,0,0) 95%, #8da389 95%),
                                            linear-gradient(175deg, rgba(0,0,0,0) 90%, #b4b07f 90%),
                                            linear-gradient( 85deg, rgba(0,0,0,0) 92%, #b4b07f 92%),
                                            linear-gradient(175deg, rgba(0,0,0,0) 85%, #c5a68e 85%),
                                            linear-gradient( 85deg, rgba(0,0,0,0) 89%, #c5a68e 89%),
                                            linear-gradient(175deg, rgba(0,0,0,0) 80%, #ba9499 80%),
                                            linear-gradient( 85deg, rgba(0,0,0,0) 86%, #ba9499 86%),
                                            linear-gradient(175deg, rgba(0,0,0,0) 75%, #9f8fa4 75%),
                                            linear-gradient( 85deg, rgba(0,0,0,0) 83%, #9f8fa4 83%),
                                            linear-gradient(175deg, rgba(0,0,0,0) 70%, #74a6ae 70%),
                                            linear-gradient( 85deg, rgba(0,0,0,0) 80%, #74a6ae 80%);' 
                        """
        }
        self.DATEFORMAT = '%Y-%m-%d %H:%M:%S'
        self.initial_date = datetime.strptime("2018-04-25 00:00:00",self.DATEFORMAT)
        self.account_type = 'all'
        self.trigger = -1
        self.periods_to_plot = ['week', 'month','quarter']
        self.history_periods = 2 # number of periods for period over period
        self.period_start_date = None
        self.period_end_date = None

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
    
    def first_date_in_quarter(self,timestamp):
        try:
            curr_quarter = int((timestamp.month - 1) / 3 + 1)
            return datetime(timestamp.year, 3 * curr_quarter - 2, 1)

        except Exception:
            logger.error('period to date', exc_info=True)
    
    def first_date_in_period(self,timestamp,period):
        try:
            if period == 'week':
                start = timestamp - timedelta(days=timestamp.weekday())
            elif period == 'month':
                start = timestamp - timedelta(days=timestamp.day)
            elif period == 'year':
                start = datetime(timestamp.year,1,1,0,0,0)
            else: #period == 'quarter':
                start = self.first_date_in_quarter(timestamp)
            return start
        except Exception:
            logger.error('period to date', exc_info=True)
        

    def period_to_date(self,df,timestamp = None,filter_col=None,cols=[],period='week'):
        try:
            if timestamp is None:
                timestamp = datetime.now()
                timestamp = datetime(timestamp.year,timestamp.month,timestamp.day,timestamp.hour,0,0)
            
            start = self.first_date_in_period(timestamp,period)
            # filter
            if filter_col is None:
                filter_col = 'block_timestamp'
            df = df[(df[filter_col] >= start) & (df[filter_col] <= timestamp)]
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
            logger.error('shift period range',exc_info=True)

    # label dates for period over period (pop)
    def label_dates_pop(self, df, period, timestamp_col):
        def df_label_dates_qtr_pop(y):
            try:
                curr_quarter = int((y.month - 1) / 3 + 1)
                start = datetime(y.year, 3 * curr_quarter - 2, 1)
                return (start - y).days
            except Exception:
                logger.error('df label quarter', exc_info=True)
        try:
            if period == 'week':
                df = df.assign(dayset=lambda x: x[timestamp_col].dt.dayofweek)
            elif period == 'month':
                df = df.assign(dayset=lambda x: x[timestamp_col].dt.day)
            elif period == 'year':
                df = df.assign(dayset=lambda x: x[timestamp_col].timetuple().tm_yday)
            else: # period == 'quarter
                df['dayset'] = df[timestamp_col].map(df_label_dates_qtr_pop)
            return df
        except Exception:
            logger.error('label data ', exc_info=True)
        
        
    def period_over_period(self,df,start_date, end_date, period,
                           history_periods=2,timestamp_col='timestamp_of_first_event'):
        try:
            # filter cols if necessary
            string = 'current '.format(period)
            df_current = df.assign(period=string)
            # label the days being compared with the same label
            df_current = self.label_dates_pop(df_current,period,timestamp_col)

            start = datetime(start_date.year,start_date.month,start_date.day,0,0,0)
            end = datetime(end_date.year,end_date.month, end_date.day,0,0,0)

            cols = list(df.columns)
            counter = 0
            if isinstance(history_periods,str):
                history_periods = int(history_periods)
            while counter < history_periods and start >= self.initial_date:
                counter += 1
                start, end = self.shift_period_range(period,start,end)
                # load data
                #logger.warning('start:end %s:%s', start, end)
                df_temp = self.load_df(start,end,cols)
                if df_temp is not None:
                    if len(df_temp) > 1:
                        string = '{} {}(s) prev'.format(counter, period)
                        # label period
                        df_temp = df_temp.assign(period=string)
                        # relabel days to get matching day of week,doy, dom, for different periods
                        df_temp = self.label_dates_pop(df_temp,period,timestamp_col)
                        #logger.warning('df temp loaded for %s previous: %s',counter,len(df_temp))

                        df_current = concat_dfs(df_current,df_temp)
            return df_current
        except Exception:
            logger.error('period over period to date',exc_info=True)

    """
     to enable comparision across period, dates must have label relative to period start
     
    """
    def split_period_into_columns(self, df,col_to_split,value_to_copy):
        try:
            for item in df[col_to_split].unique():
                df[item] = df.apply(lambda x: x[value_to_copy] if x[col_to_split] == item else 0, axis=1)
            #logger.warning('split period into columns:%s', df.head(10))
            return df
        except Exception:
            logger.error('split period into column', exc_info=True)

    # -----------------------  UPDATERS  ------------------------------------------
    def notification_updater(self, text):
        txt = """<div style="text-align:center;background:black;width:100%;">
                <h4 style="color:#fff;">
                {}</h4></div>""".format(text)
        self.notification_div.text = txt

    """
        update the section labels on the page

    """
    def section_header_updater(self,section):
        label = self.account_type
        if label != 'all':
            label = label+'s'
        if section == 'cards':
            text = "New accounts:{}".format(label)
        elif section == 'pop':
            text = "Period over period:{}".format(label)
        txt = """<h2 style="color:#4221cc;">{}-----------------------------------------------------------------</h2>"""\
            .format(text)
        self.section_headers[section].text = txt



