import csv
from os.path import join, dirname

from scripts.utils.mylogger import mylogger
from scripts.utils.poolminer import  make_tier1_list,\
    make_tier2_list, is_tier2_in_memory, is_tier1_in_memory
from scripts.utils.myutils import tab_error_flag
from scripts.utils.mytab import Mytab, DataLocation
from config import dedup_cols, columns as cols
from tornado import gen
from concurrent.futures import ThreadPoolExecutor
from tornado.locks import Lock


import datashader as ds
from bokeh.layouts import layout, column, row, gridplot, WidgetBox
from bokeh.models import ColumnDataSource, HoverTool, Panel, Range1d, CustomJS
import gc
from bokeh.io import curdoc
from bokeh.models.widgets import DateRangeSlider, TextInput, Slider, Div, \
    DatePicker, TableColumn, DataTable, Button, Select
from holoviews import streams
from holoviews.streams import Stream, RangeXY, RangeX, RangeY, Pipe
from pdb import set_trace
import hvplot.dask
import hvplot.pandas

import holoviews as hv, param, dask.dataframe as dd
from holoviews.operation.datashader import rasterize, shade, datashade
from datetime import datetime
import numpy as np
import pandas as pd
import dask as dd
from pdb import set_trace
from holoviews import streams

from dask.distributed import Client
from dask import visualize, delayed

import holoviews as hv
import time
from tornado.gen import coroutine

lock = Lock()
executor = ThreadPoolExecutor()
logger = mylogger(__file__)

hv.extension('bokeh', logo=False)
tables = {}
tables['block'] = 'block'
tables['transaction'] = 'transaction'

menu = [str(x * 0.5) for x in range(0, 80)]
menu_blocks_mined = [str(x) if x > 0 else '' for x in range(0,50)]


@coroutine
def poolminer_tab():
    # source for top N table
    tier2_src = ColumnDataSource(data= dict(
                to_addr=[],
                approx_value=[]))

    tier1_src = ColumnDataSource(data=dict(
        from_addr=[],
        block_number=[],
        approx_value=[]))

    class Thistab(Mytab):
        block_tab = Mytab('block', cols, dedup_cols)
        transaction_tab = Mytab('transaction',cols, dedup_cols,
                                query_cols=['block_date',
                                            'transaction_hash','from_addr',
                                            'to_addr','approx_value'])

        def __init__(self, table, cols=[], dedup_cols=[], query_cols=[]):
            Mytab.__init__(self, table, cols, dedup_cols, query_cols)
            self.table = table
            self.tier1_df = self.df
            self.tier2_df = self.df
            self.threshold_tier2_received = .5
            self.threshold_tx_paid_out = 1
            self.threshold_blocks_mined = 1
            self.tier2_miners_list = []
            self.tier1_miners_list = []
            self.tier1_miners_activated = False

        def df_loaded_check(self,start_date, end_date):
            # check to see if block_tx_warehouse is loaded
            data_location = self.is_data_in_memory(start_date, end_date)
            if data_location == DataLocation.IN_MEMORY:
                #logger.warning('warehouse already loaded:%s', self.df.tail(40))
                pass
            elif data_location == DataLocation.IN_REDIS:
                self.load_data(start_date, end_date)
            else:
                # load the two tables and make the block
                self.block_tab.load_data(start_date, end_date)
                self.transaction_tab.load_data(start_date, end_date)
                self.load_data(start_date, end_date, df_tx=self.transaction_tab.df,
                               df_block=self.block_tab.df)

        def tier_1_loaded_check(self,start_date,end_date):
            # TIER 1 MINERS
            tier_1_miners_list = is_tier1_in_memory(start_date, end_date,
                                                   self.threshold_tx_paid_out,
                                                   self.threshold_blocks_mined)
            # generate the list if necessary
            if tier_1_miners_list is None:
                self.df_loaded_check(start_date, end_date)

                self.filter_df(start_date, end_date)
                values = {'approx_value': 0, 'to_addr': 'unknown',
                          'from_addr': 'unknown', 'block_number': 0}
                self.df1 = self.df1.fillna(values)
                # with data in hand, make the list
                tier_1_miners_list = make_tier1_list(self.df1, start_date, end_date,
                                                     self.threshold_tx_paid_out,
                                                     self.threshold_blocks_mined)
            return tier_1_miners_list

        def load_this_data(self, start_date, end_date,threshold_tx_paid_out,
                           threshold_blocks_mined):
            if isinstance(threshold_blocks_mined,str):
                threshold_blocks_mined = int(threshold_blocks_mined)
            if isinstance(threshold_tx_paid_out,str):
                threshold_tx_paid_out = float(threshold_tx_paid_out)

            self.threshold_tx_paid_out = threshold_tx_paid_out
            self.threshold_blocks_mined = threshold_blocks_mined
            end_date = datetime.combine(end_date, datetime.min.time())
            start_date = datetime.combine(start_date, datetime.min.time())

            tier_1_miners_list = self.tier_1_loaded_check(start_date,end_date)

            return self.make_tier1_table(tier_1_miners_list,start_date,end_date)

        def make_tier1_table(self,tier1_miners_list,start_date,end_date):
            try:
                logger.warning("tier 1 triggered:%s:%s",start_date,end_date)
                # ensure tier1 is loaded
                self.df_loaded_check(start_date,end_date)
                self.filter_df(start_date,end_date)
                # get tier1 miners list
                logger.warning("merged column in make tier1:%s",self.df1.columns.tolist())

                # load the dataframe
                self.df1['from_addr'] = self.df1['from_addr'].astype(str)
                self.tier1_df = self.df1.groupby(['from_addr'])\
                    .agg({'approx_value':'sum',
                          'block_number':'count'}).reset_index()
                self.tier1_df = self.tier1_df[self.tier1_df.from_addr.isin(tier1_miners_list)]


                # for csv export
                values = {'approx_value': 0, 'from_addr': 'unknown',
                          'block_number': 0}

                self.tier1_df = self.tier1_df.fillna(values)

                #self.tier1_df['from_addr'] = self.tier1_df['from_addr'].map(lambda x: self.poolname_verbose(x))
                self.tier1_df['from_addr'] = self.tier1_df['from_addr'].map(self.poolname_verbose)
                #logger.warning('POOLNAMES IN from_addr:%s',self.tier1_df['from_addr'])

                # make source for table and csv
                new_data = {}
                tier1_df = self.tier1_df.compute()

                for x in ['from_addr', 'approx_value', 'block_number']:
                    if x == 'block_date':
                        tier1_df[x] = tier1_df[x].dt.strftime('%Y-%m-%d')
                    new_data[x] = tier1_df[x].tolist()

                tier1_src.stream(new_data, rollover=len(tier1_df))
                logger.warning("TIER 1 Calculations finished:%s", len(tier1_df.index))
                columns = [
                    TableColumn(field="from_addr", title="Address"),
                    TableColumn(field="approx_value", title="Value"),
                    TableColumn(field="block_number", title="# of blocks"),

                ]
                return DataTable(source=tier1_src,columns=columns,width=600,height=1400)
                '''
                return tier1_df.hvplot.table(columns=['miner_address', 'block_date',
                                                  'block_number', 'approx_value'], width=800)
                '''
            except Exception:
                logger.warning("make tier 1 table",exc_info=True)
            del new_data
            del tier1_df
            gc.collect()

        def date_to_str(self, ts):

            if isinstance(ts,datetime):
                return datetime.strftime(ts,"%Y-%m-%d")

            return ts

        def make_tier2_table(self,start_date,end_date,
                             threshold_tier2_received,
                             threshold_tx_paid_out,
                             threshold_blocks_mined):
            try:
                logger.warning("tier 2 triggered:%s",threshold_tier2_received)
                if isinstance(threshold_tier2_received,str):
                    threshold_tier2_received = float(threshold_tier2_received)
                if isinstance(threshold_blocks_mined, str):
                    threshold_blocks_mined = int(threshold_blocks_mined)
                if isinstance(threshold_tx_paid_out,str):
                    threshold_tx_paid_out = float(threshold_tx_paid_out)
                self.threshold_tx_paid_out = threshold_tx_paid_out
                self.threshold_blocks_mined = threshold_blocks_mined
                self.threshold_tier2_received = threshold_tier2_received

                tier2_miners_list = is_tier2_in_memory(start_date, end_date,
                                                       threshold_tx_paid_out,
                                                       threshold_blocks_mined)
                # generate the list if necessary
                if tier2_miners_list is None:
                    # get tier 1 miners list
                    tier_1_miners_list = self.tier_1_loaded_check(start_date,end_date)

                    tier2_miners_list = \
                        make_tier2_list(self.df, start_date, end_date,
                                        tier_1_miners_list,
                                        threshold_tier2_received=threshold_tier2_received,
                                        threshold_tx_paid_out=threshold_tx_paid_out,
                                        threshold_blocks_mined_per_day=threshold_blocks_mined)

                # load the dataframe
                self.df1['to_addr'] = self.df1['to_addr'].astype(str)
                self.tier2_df = self.df1.groupby(['to_addr']) \
                    .agg({'approx_value': 'sum'}).reset_index()
                self.tier2_df = self.tier2_df[self.tier2_df.to_addr.isin(tier2_miners_list)]

                # for csv export
                values = {'approx_value': 0, 'to_addr': 'unknown',
                          'from_addr': 'unknown'}

                self.tier2_df = self.tier2_df.fillna(values)
                tier2_df = self.tier2_df.compute()
                new_data={}
                for x in ['to_addr','approx_value']:
                    if x == 'block_date':
                        tier2_df[x] = tier2_df[x].dt.strftime('%Y-%m-%d')
                    new_data[x] = tier2_df[x].tolist()

                logger.warning("TIER 2 Calculations finished:%s", len(tier2_df.index))
                tier2_src.stream(new_data, rollover=len(tier2_df))
                columns = [
                    TableColumn(field="to_addr", title="Address"),
                    TableColumn(field="approx_value", title="Value"),

                ]
                return DataTable(source=tier2_src, columns=columns, width=400, height=1400)
            except Exception:
                logger.warning("make tier 2 table",exc_info=True)


            del new_data
            del tier2_df
            gc.collect()

    def update_threshold_tier_2_received(attrname, old, new):
        thistab.make_tier2_table(datepicker_start.value, datepicker_end.value,
                                 select_tx_received.value,
                                 select_tx_paid_out.value,
                                 select_blocks_mined.value)

    def update(attr, old, new):
        thistab.tier1_miners_activated = True
        thistab.load_this_data(datepicker_start.value, datepicker_end.value,
                               select_tx_paid_out.value,
                               select_blocks_mined.value)

        thistab.make_tier2_table(datepicker_start.value, datepicker_end.value,
                                 select_tx_received.value,
                                 select_tx_paid_out.value,
                                 select_blocks_mined.value)
        thistab.tier1_miners_activated = False


    try:
        query_cols=['block_date','block_number','to_addr',
                    'from_addr','miner_address','approx_value','transaction_hash']
        thistab = Thistab('block_tx_warehouse',query_cols=query_cols)

        # STATIC DATES
        # format dates
        first_date_range = "2018-04-23 00:00:00"
        first_date_range = datetime.strptime(first_date_range, "%Y-%m-%d %H:%M:%S")
        last_date_range = datetime.now().date()
        first_date = datetime.strptime("2018-11-01",'%Y-%m-%d')
        last_date = datetime.now().date()

        thistab.load_this_data(first_date,last_date,
                               thistab.threshold_tx_paid_out,
                               thistab.threshold_blocks_mined)

        thistab.make_tier2_table(first_date,last_date,
                                 thistab.threshold_tier2_received,
                                 thistab.threshold_tx_paid_out,
                                 thistab.threshold_blocks_mined)

        # MANAGE STREAM
        # date comes out stream in milliseconds

        # CREATE WIDGETS
        datepicker_start = DatePicker(title="Start", min_date=first_date_range,
                                      max_date=last_date_range, value=first_date)
        datepicker_end = DatePicker(title="End", min_date=first_date_range,
                                    max_date=last_date_range, value=last_date)
        select_tx_received = Select(title='Threshold, Tier2: daily tx received',
                                    value=str(thistab.threshold_tier2_received),
                                    options=menu)
        select_blocks_mined = Select(title='Threshold, Tier1: blocks mined',
                                     value=str(thistab.threshold_blocks_mined),
                                     options=menu_blocks_mined)
        select_tx_paid_out = Select(title='Threshold, Tier1: tx paid out',
                                    value=str(thistab.threshold_tx_paid_out),
                                    options=menu)

        columns = [
            TableColumn(field="from_addr", title="Address"),
            TableColumn(field="approx_value", title="Value"),
            TableColumn(field="block_number", title="# of blocks"),

        ]
        tier1_table = DataTable(source=tier1_src, columns=columns, width=600, height=1400)

        columns = [
            TableColumn(field="to_addr", title="Address"),
            TableColumn(field="approx_value", title="Value"),

        ]
        tier2_table = DataTable(source=tier2_src, columns=columns, width=400, height=1400)


        # handle callbacks
        datepicker_start.on_change('value', update)
        datepicker_end.on_change('value', update)
        select_tx_received.on_change("value", update_threshold_tier_2_received)
        select_blocks_mined.on_change('value', update)
        select_tx_paid_out.on_change('value', update)

        download_button_1 = Button(label='Save Tier 1 miners list to CSV', button_type="success")
        download_button_2 = Button(label='Save Tier 2 miners list to CSV', button_type="success")

        download_button_1.callback = CustomJS(args=dict(source=tier1_src),
                                              code=open(join(dirname(__file__),
                                                             "../../assets/js/tier1_miners_download.js")).read())

        download_button_2.callback = CustomJS(args=dict(source=tier2_src),
                                              code=open(join(dirname(__file__),
                                                             "../../assets/js/tier2_miners_download.js")).read())

        # COMPOSE LAYOUT
        # put the controls in a single element
        controls_left = WidgetBox(
            datepicker_start,
            select_tx_paid_out,
            select_blocks_mined,
            download_button_1)

        controls_right = WidgetBox(
            datepicker_end,
            select_tx_received,
            download_button_2)

        # create the dashboard
        spacing_div = thistab.spacing_div(width=200,height=400)
        grid = gridplot([[controls_left,controls_right],
                         [tier1_table, tier2_table]])

        # Make a tab with the layout
        tab = Panel(child=grid, title='Poolminers')
        return tab

    except Exception:
        logger.error('rendering err:',exc_info=True)
        return tab_error_flag('poolminer')


