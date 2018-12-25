from os.path import join, dirname

from scripts.utils.mylogger import mylogger
from scripts.utils.poolminer import make_poolminer_warehouse, make_tier1_list,\
    make_tier2_list, warehouse_needed
from scripts.utils.myutils import tab_error_flag
from scripts.utils.mytab import Mytab
from config import dedup_cols, columns as cols
from tornado import gen
from concurrent.futures import ThreadPoolExecutor
from tornado.locks import Lock


import datashader as ds
from bokeh.layouts import layout, column, row, gridplot, WidgetBox
from bokeh.models import ColumnDataSource, HoverTool, Panel, Range1d, Button, CustomJS
import gc
from bokeh.io import curdoc
from bokeh.models.widgets import DateRangeSlider, TextInput, Slider, Div, DatePicker
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

@coroutine
def poolminer_tab():
    # source for top N table
    tier1_src = ColumnDataSource(data= dict(
                block_date=[],
                miner_address=[],
                approx_value=[],
                block_number=[]))

    class Thistab(Mytab):
        block_tab = Mytab('block', cols, dedup_cols,
                          query_cols=['block_date','block_number',
                                      'miner_address','block_timestamp',
                                      'transaction_hashes'])
        transaction_tab = Mytab('transaction',cols, dedup_cols,
                                query_cols=['block_date',
                                            'transaction_hash','from_addr',
                                            'to_addr','approx_value'])
        def __init__(self, table, cols=[], dedup_cols=[], query_cols=[]):
            Mytab.__init__(self, table, cols, dedup_cols, query_cols)
            self.table = table
            self.df = None
            self.tier1_df1 = None

        def load_this_data(self, start_date, end_date):
            end_date = datetime.combine(end_date, datetime.min.time())
            start_date = datetime.combine(start_date, datetime.min.time())
            # check to see if table loaded
            block_tx_needed = thistab.load_data(start_date,end_date)
            if block_tx_needed:
                # load the two tables
                self.block_tab.load_data(start_date,end_date)
                self.transaction_tab.load_data(start_date,end_date)
                self.df = make_poolminer_warehouse(
                                                   self.transaction_tab.df,
                                                   self.block_tab.df,
                                                   start_date,
                                                   end_date)
            else:
                # load the warehouse
                logger.warning('warehouse already loaded:%s',self.df.tail(40))
            return self.make_tier1_table(start_date, end_date)

        def make_tier1_table(self,start_date,end_date):
            # get tier1 miners list
            tier1_miners_list = make_tier1_list(self.df,start_date,end_date)
            logger.warning("tier 1 miners:%s",tier1_miners_list)
            # filter dataframe to get list
            self.tier1_df = self.df[self.df.from_addr.isin(tier1_miners_list)]
            self.tier1_df = self.tier1_df.groupBy(['miner_address','block_date'])\
                .agg({'approx_value':'sum',
                      'block_number':'count'}).reset_index()

            new_data = dict(
                block_date=self.tier1_df.block_date,
                miner_address=self.tier1_df.miner_address,
                approx_value=self.tier1_df.approx_value,
                block_number=self.tier1_df.block_number
            )
            # src.stream
            tier1_src.stream(new_data)
            return self.tier1_df.hvplot.table(columns=['miner_address','block_date',
                                      'block_number','approx_value'],width=800)

        # notify the holoviews stream of the slider updates

    def update_start_date(attrname, old, new):
        stream_start_date.event(start_date=new)

    def update_end_date(attrname, old, new):
        stream_end_date.event(end_date=new)


    try:
        #query_cols=['block_time','difficulty','block_date','block_number']
        thistab = Thistab('block_tx_warehouse')

        # STATIC DATES
        # format dates
        first_date_range = "2018-04-23 00:00:00"
        first_date_range = datetime.strptime(first_date_range, "%Y-%m-%d %H:%M:%S")
        last_date_range = datetime.now().date()
        last_date = "2018-05-23 00:00:00"
        last_date = datetime.strptime(last_date, "%Y-%m-%d %H:%M:%S")
        thistab.load_this_data(first_date_range,last_date)


        # MANAGE STREAM
        # date comes out stream in milliseconds
        stream_start_date = streams.Stream.define('Start_date',
                                                  start_date=first_date_range)()
        stream_end_date = streams.Stream.define('End_date', end_date=last_date)()


        # CREATE WIDGETS
        datepicker_start = DatePicker(title="Start", min_date=first_date_range,
                                      max_date=last_date_range, value=first_date_range)
        datepicker_end = DatePicker(title="End", min_date=first_date_range,
                                    max_date=last_date, value=last_date)

        # declare plots
        dmap_miner1 = hv.DynamicMap(
            thistab.load_this_data, streams=[stream_start_date,
                                             stream_end_date]) \
            .opts(plot=dict(width=800, height=1400))

        # handle callbacks
        datepicker_start.on_change('value', update_start_date)
        datepicker_end.on_change('value', update_end_date)

        download_button = Button(label='Save Table to CSV', button_type="success")
        download_button.callback = CustomJS(args=dict(source=tier1_src),
                                            code=open(join(dirname(__file__),
                                                           "../../assets/js/tier1_miner_download.js"))
                                            .read())


        # Render layout to bokeh server Document and attach callback
        renderer = hv.renderer('bokeh')
        miner1_plot = renderer.get_plot(dmap_miner1)

        # COMPOSE LAYOUT
        # put the controls in a single element
        controls = WidgetBox(
            datepicker_start, datepicker_end,
            download_button)

        # create the dashboard
        grid = gridplot([[controls],[miner1_plot.state]])

        # Make a tab with the layout
        tab = Panel(child=grid, title='Poolminers')
        return tab

    except Exception:
        logger.error('rendering err:',exc_info=True)