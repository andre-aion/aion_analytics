import gc
from math import inf
from os.path import join, dirname

import pandas as pd
import numpy as np
from dask import dataframe as dd
from datetime import datetime,timedelta,date
from dateutil.relativedelta import relativedelta

from scripts.databases.pythonClickhouse import PythonClickhouse
from scripts.databases.pythonRedis import PythonRedis
from scripts.utils.mylogger import mylogger
from scripts.utils.myutils import concat_dfs
from static.css.KPI_interface import KPI_card_css

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
        'history_periods': ['1','2','3','4','5','6','7','8','9','10'],
        'developer_adoption_variables': ['aion_fork', 'aion_watch']
    }
    def __init__(self,table,cols):
        self.df = None
        self.ch = PythonClickhouse('aion')
        self.redis = PythonRedis()
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
        self.KPI_card_css = KPI_card_css
        self.DATEFORMAT = '%Y-%m-%d %H:%M:%S'
        self.initial_date = datetime.strptime("2018-04-25 00:00:00",self.DATEFORMAT)
        self.account_type = 'all'
        self.trigger = -1
        self.periods_to_plot = ['week', 'month','quarter']
        self.history_periods = 2 # number of periods for period over period
        self.period_start_date = None
        self.period_end_date = None

        self.checkboxgroup = {}
        self.sig_effect_dict = {}
        self.name = ''
        self.redis_stat_sig_key = 'adoption_features:' + self.name

        # make block timestamp the index
    def load_df(self,start_date,end_date,cols,timestamp_col='timestamp_of_first_event'):
        try:

            if isinstance(end_date,date):
                end_date = datetime.combine(end_date,datetime.min.time())
            if isinstance(start_date,date):
                start_date = datetime.combine(start_date,datetime.min.time())
            end_date += timedelta(days=1)
            temp_cols = cols.copy()

            if 'amount' not in temp_cols:
                temp_cols.append('amount')

            df = self.ch.load_data(self.table, temp_cols, start_date, end_date,timestamp_col)
            logger.warning('line 79:%s columns before value filter',df.columns)
            # filter out the double entry
            #df = df[df['value'] >= 0]
            return df[cols]
            #df[timestamp_col] = df[timestamp_col].map(lambda x: clean_dates_from_db(x))
        except Exception:
            logger.error('load df',exc_info=True)

    def reset_checkboxes(self, value='all', checkboxgroup=''):
        try:
            self.checkboxgroup[checkboxgroup].value = value
        except Exception:
            logger.error('reset checkboxes', exc_info=True)

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
            elif period == 'quarter':
                start = self.first_date_in_quarter(timestamp)
            return start
        except Exception:
            logger.error('period to date', exc_info=True)

    def period_to_date(self, df, timestamp = None, timestamp_filter_col=None, cols=[], period='week'):
        try:
            if timestamp is None:
                timestamp = datetime.now()
                timestamp = datetime(timestamp.year,timestamp.month,timestamp.day,timestamp.hour,0,0)
            
            start = self.first_date_in_period(timestamp,period)
            # filter
            if timestamp_filter_col is None:
                timestamp_filter_col = 'block_timestamp'
            df = df[(df[timestamp_filter_col] >= start) & (df[timestamp_filter_col] <= timestamp)]
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
                return abs((start - y).days)
            except Exception:
                logger.error('df label quarter', exc_info=True)
        try:
            if period == 'week':
                df = df.assign(dayset=lambda x: x[timestamp_col].dt.dayofweek)
                logger.warning('df after WEEK:%s',df.head(10))

            elif period == 'month':
                df = df.assign(dayset=lambda x: x[timestamp_col].dt.day)
            elif period == 'year':
                df = df.assign(dayset=lambda x: x[timestamp_col].timetuple().tm_yday)
            elif period == 'quarter':
                df['dayset'] = df[timestamp_col].map(df_label_dates_qtr_pop)
                logger.warning('df after QUARTER:%s',df.head(10))
            return df
        except Exception:
            logger.error('label data ', exc_info=True)
        
        
    def period_over_period(self,df,start_date, end_date, period,
                           history_periods=2,timestamp_col='timestamp_of_first_event'):
        try:
            # filter cols if necessary
            string = 'current {}'.format(period)
            string = '0 {}(s) prev(current)'.format(period)
            df_current = df.assign(period=string)
            # label the days being compared with the same label
            df_current = self.label_dates_pop(df_current,period,timestamp_col)

            # zero out time information
            start = datetime(start_date.year,start_date.month,start_date.day,0,0,0)
            end = datetime(end_date.year,end_date.month, end_date.day,0,0,0)

            cols = list(df.columns)
            counter = 0
            if isinstance(history_periods,str):
                history_periods = int(history_periods)
            # make dataframes for request no. of periods
            start, end = self.shift_period_range(period, start, end)
            while counter < history_periods and start >= self.initial_date:
                counter += 1
                # load data
                if period == 'quarter':
                    logger.warning('start:end %s:%s', start, end)
                df_temp = self.load_df(start,end,cols,timestamp_col)
                if df_temp is not None:
                    if len(df_temp) > 1:
                        string = '{} {}(s) prev'.format(counter, period)
                        # label period
                        df_temp = df_temp.assign(period=string)
                        # relabel days to get matching day of week,doy, dom, for different periods
                        df_temp = self.label_dates_pop(df_temp,period,timestamp_col)
                        #logger.warning('df temp loaded for %s previous: %s',counter,len(df_temp))

                        df_current = concat_dfs(df_current,df_temp)
                        del df_temp
                        gc.collect()
                start,end = self.shift_period_range(period,start,end)
            return df_current
        except Exception:
            logger.error('period over period',exc_info=True)

    """
     To enable comparision across period, dates must have label relative to period start.
     Place dates in columns to be able to plot multi-line/bar graphs
     
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

    # -------------------- CALCULATE KPI's DEVELOPED FROM VARIABLES WITH STATITICALLY SIGNIFICANT EFFECT
    def calc_sig_effect_card_data(self, df, variable_of_interest):
        try:
            # load statistically significant variables
            sig_variables = self.redis.simple_load(self.redis_stat_sig_key)
            self.sig_effect_dict = {}
            if sig_variables is not None:
                if 'features' in sig_variables.keys():
                    if len(sig_variables['features']) > 0:
                        tmp_df = df[[variable_of_interest,sig_variables]]
                        numer = tmp_df[variable_of_interest].mean()
                        for var in sig_variables:
                            tmp = 0
                            if isinstance(numer,float) or isinstance(numer,int):
                                if numer != 0:
                                    denom = tmp_df[var].mean()
                                    if isinstance(denom, float) or isinstance(denom,int):
                                        tmp = round(numer/denom,3)
                            # add metrics based on variables
                            title = "{} per {}".format(variable_of_interest,var)
                            self.sig_effect_dict[var] = {
                                'title' : title,
                                'point_estimate':tmp
                            }
            logger.warning('%s sig_effect_data:%s',variable_of_interest,self.sig_effect_dict)
        except Exception:
            logger.error('make sig effect columns', exc_info=True)
