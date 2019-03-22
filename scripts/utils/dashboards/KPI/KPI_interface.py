import pandas as pd
from dask import dataframe as dd
from datetime import datetime,timedelta,date
from dateutil.relativedelta import relativedelta

from scripts.storage.pythonClickhouse import PythonClickhouse
from scripts.utils.mylogger import mylogger

import hvplot.pandas
import hvplot.dask

logger = mylogger(__file__)

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

        # make block timestamp the index
    def load(self,start_date,end_date):
        try:
            if isinstance(end_date,date):
                end_date = datetime.combine(end_date,datetime.time.min())
            if isinstance(start_date,date):
                start_date = datetime.combine(start_date,datetime.time.min())
            self.df = self.ch.load_data(self.table, self.cols, start_date, end_date)
        except Exception:
            logger.error('load df',exc_info=True)

    def period_to_date(self,df,timestamp = None,cols=[],period='week'):
        try:
            if timestamp is None:
                timestamp = datetime.now()
            if period == 'week':
                start = timestamp - timedelta(days=timestamp.weekday())
            elif period == 'month':
                start = timestamp.replace(day=1)
            elif period == 'year':
                start = timestamp.replace(day=1,month=1)
            elif period == 'quarter':
                curr_quarter = int((timestamp.month - 1) / 3 + 1)
                start = datetime(timestamp.year, 3 * curr_quarter - 2, 1)

            # filter
            df = df.loc[start:self.now]
            if len(cols) >0:
                df = df[cols]
            return df
        except Exception:
            logger.error('period to date',exc_info=True)


    def period_over_period(self,df,startdate,enddate,period_count=1,period='month',cols=[]):
        try:
            # filter cols if necessary
            if len(cols) > 0:
                df = df[cols]
            df_current = df.loc[startdate:enddate]
            df_current = df_current.assign(period='current')
            counter = 0
            start = startdate
            end = enddate
            while counter < period_count and start >= df.index.max:
                counter += 1
                if period == 'week':
                    start = start - timedelta(days=7)
                    end = end - timedelta(days=7)
                elif period == 'month':
                    start = start - timedelta(month=1)
                    end = end - timedelta(month=1)
                elif period == 'year':
                    start = start.replace(year=1)
                    end = end.replace(year=1)
                elif period == 'quarter':
                    start = start - timedelta(month=3)
                    end = end - timedelta(month=3)
                df_temp = df.loc[start:end]
                string = '{} {} prev'.format(counter, period)
                df_temp = df_temp.assign(period=string)

                df_current = dd.concat([df_current,df_temp],axis=0)
                logger.warning('counter=%s,start=%s',counter,start)
            return df_current
        except Exception:
            logger.error('period over period to date',exc_info=True)


    def graph_period_over_period(self,df,startdate,enddate,period_count=1,period='month',cols=[]):
        try:
            df = self.period_over_period(df,startdate,enddate,period_count,period,cols)
            p = df.hvplot.box(cols[0], by=period)
            if len(cols) > 1:
                for idx,col in enumerate(cols):
                    if idx > 0:
                        p += df.hvplot.box(col, by=period)

            return p

        except Exception:
            logger.error('period over period to date', exc_info=True)

    def clean_data(self, df):
        df = df.fillna(0)
        df[df == -inf] = 0
        df[df == inf] = 0
        return df
