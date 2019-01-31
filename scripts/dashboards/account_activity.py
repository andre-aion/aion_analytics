from os.path import join, dirname

from holoviews import streams

from config.df_construct_config import table_dict
from scripts.utils.mylogger import mylogger
from scripts.utils.dashboards.poolminer import make_tier1_list,\
    make_tier2_list, is_tier2_in_memory, is_tier1_in_memory
from scripts.utils.myutils import tab_error_flag, datetime_to_date
from scripts.utils.dashboards.mytab import Mytab
from concurrent.futures import ThreadPoolExecutor
from tornado.locks import Lock

from bokeh.layouts import gridplot, WidgetBox
from bokeh.models import ColumnDataSource, Panel, CustomJS
import gc
from bokeh.models.widgets import Div, \
    DatePicker, TableColumn, DataTable, Button, Select, Paragraph

from datetime import datetime, timedelta

import holoviews as hv
import hvplot.pandas
import hvplot.dask
from tornado.gen import coroutine
from numpy import inf

logger = mylogger(__file__)

hv.extension('bokeh', logo=False)
renderer = hv.renderer('bokeh')

menu_period = ['D','W','H''M']

@coroutine
def account_activity_tab():
    class Thistab(Mytab):
        def __init__(self, table,cols=[], dedup_cols=[]):
            Mytab.__init__(self, table, cols, dedup_cols)
            self.table = table
            self.cols = cols
            self.period = menu_period[0]
            self.trigger = 0
            txt = """<div style="text-align:center;background:black;width:100%;">
                                                               <h1 style="color:#fff;">
                                                               {}</h1></div>""".format('Welcome')
            self.notification_div = Div(text=txt,width=1400,height=20)

        def clean_data(self, df):
            df.fillna(0, inplace=True)
            df[df == -inf] = 0
            df[df == inf] = 0
            return df

        def load_df(self,start_date, end_date):
            try:
                # make block_timestamp into index
                self.df_load(start_date,end_date)
                #self.df1 = self.df1.groupby(['block_timestamp','address']).agg({'value':'sum'})
                #self.df1 = self.df1.reset_index()
                self.df1 = self.df1.set_index('block_timestamp',sorted=True)
                self.df1 = self.df1.resample(self.period).sum()
                self.df1 = self.df1.compute()
                #self.df1 = self.df1.pct_change(fill_method='ffill')
                self.df1 = self.clean_data(self.df1)

            except Exception:
                logger.warning('load df',exc_info=True)

        def plot_account_balance(self,launch=-1):
            try:
                logger.warning('before plot:%s',self.df1.tail(60))
                # make block_timestamp into index
                return self.df1.hvplot.bar()
            except Exception:
                logger.warning('load df',exc_info=True)


    def update(attrname, old, new):
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.load_df(datepicker_start.value,datepicker_end.value)
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        thistab.notification_updater("Ready.")

    def update_resample(attr,old,new):
        thistab.notification_updater("Calculations in progress! Please wait.")
        #thistab.df1 = thistab.df.set_index('block_timestamp')
        thistab.df1 = thistab.df[['value','block_timestamp']]
        thistab.period = period_select.value
        if period_select.value == 'month':
            thistab.df1 = thistab.df1.groupby(thistab.df1.block_timestamp.month)
        if period_select.value == 'day':
            thistab.df1 = thistab.df1.groupby(thistab.df1.block_timestamp.day)
        if period_select.value == 'week':
            thistab.df1 = thistab.df1.groupby(thistab.df1.block_timestamp.week)

        thistab.notification_updater("Ready!")


    try:
        cols = ['address','block_timestamp','value']
        thistab = Thistab(table='account_activity',cols=cols)
        # STATIC DATES
        # format dates
        first_date_range = "2018-04-23 00:00:00"
        first_date_range = datetime.strptime(first_date_range, "%Y-%m-%d %H:%M:%S")
        last_date_range = datetime.now().date()
        last_date = datetime.now().date()
        first_date = datetime_to_date(last_date - timedelta(days=60))

        thistab.load_df(first_date, last_date)


        # MANAGE STREAM
        # date comes out stream in milliseconds
        stream_launch = streams.Stream.define('Launch',launch=True)()

        # CREATE WIDGETS
        datepicker_start = DatePicker(title="Start", min_date=first_date_range,
                                      max_date=last_date_range, value=first_date)
        datepicker_end = DatePicker(title="End", min_date=first_date_range,
                                    max_date=last_date_range, value=last_date)

        period_select = Select(title='Select aggregation period',
                               value='day',
                               options=menu_period)

        # --------------------- PLOTS----------------------------------
        hv_account_balance = hv.DynamicMap(thistab.plot_account_balance,
                                           streams=[stream_launch],
                                           datashade=True).opts(plot=dict(width=1200, height=400))


        account_balance = renderer.get_plot(hv_account_balance)


        # handle callbacks
        datepicker_start.on_change('value', update)
        datepicker_end.on_change('value', update)
        period_select.on_change('value',update_resample)



        # COMPOSE LAYOUT
        # put the controls in a single element
        controls = WidgetBox(
            datepicker_start,
            datepicker_end,
            period_select)


        # create the dashboards
        grid = gridplot([
            [thistab.notification_div],
             [controls],
            [account_balance.state]
            ])

        # Make a tab with the layout
        tab = Panel(child=grid, title='Account activity')
        return tab

    except Exception:
        logger.error('rendering err:', exc_info=True)
        return tab_error_flag(thistab.table)
