from scripts.utils.mylogger import mylogger
from scripts.utils.hashrate import calc_hashrate
from scripts.utils.myutils import get_initial_blocks, tab_error_flag
from scripts.utils.mytab import Mytab, DataLocation
from config import dedup_cols, columns as cols
from tornado import gen
from concurrent.futures import ThreadPoolExecutor
from tornado.locks import Lock


import datashader as ds
from bokeh.layouts import layout, column, row, gridplot, WidgetBox
from bokeh.models import ColumnDataSource, HoverTool, Panel, Range1d
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
table = 'block'

@coroutine
def hashrate_tab():

    class Thistab(Mytab):
        def __init__(self, table, cols, dedup_cols, query_cols):
            Mytab.__init__(self, table, cols, dedup_cols, query_cols)

        def load_this_data(self, start_date, end_date, bcount=10):
            self.locals['blockcount'] = bcount
            data_in_memory = self.is_data_in_memory(start_date,end_date)
            if data_in_memory != DataLocation.IN_MEMORY:
                self.load_data(start_date, end_date)
            self.difficulty_plot()
            return self.hashrate_plot(bcount)

        def hashrate_plot(self,bcount):
            try:
                df1 = calc_hashrate(self.df, bcount)
                #curve = hv.Curve(df, kdims=['block_number'], vdims=['hashrate'])\
                    #.options(width=1000,height=600)
                curve = df1.hvplot.line('block_number', 'hashrate',
                                        title='Hashrate',width=1000, height=600)
                del df1
                gc.collect()
                return curve
            except Exception:
                logger.error('hashrate_plot:',exc_info=True)

        def difficulty_text(self):  # Make a tab with the layout
            div = Div(text="""Welcome to Aion difficulty tab.""",
                      width=300, height=100)

            return div

        def difficulty_plot(self):
            try:
                #logger.warning("DF in difficulty: %s",self.df)
                p = self.df.hvplot.line(x='block_number', y='difficulty',
                                        title='Difficulty')
                return p
            except Exception:
                logger.error('plot error:', exc_info=True)

    def update_slider(attrname, old, new):
        # notify the holoviews stream of the slider update
        thistab.locals['blockcount']=new
        stream_blockcount.event(bcount=new)

    # notify the holoviews stream of the slider updates
    def update_start_date(attrname, old, new):
        stream_start_date.event(start_date=new)

    def update_end_date(attrname, old, new):
        stream_end_date.event(end_date=new)

    try:
        table='block'
        query_cols=['block_time','difficulty','block_date','block_number']
        thistab = Thistab(table, cols, dedup_cols, query_cols)
        thistab.locals['blockcount'] = 10

        # STATIC DATES
        # format dates
        first_date_range = "2018-04-23 00:00:00"
        first_date_range = datetime.strptime(first_date_range, "%Y-%m-%d %H:%M:%S")
        last_date_range = datetime.now().date()
        last_date = "2018-05-23 00:00:00"
        last_date = datetime.strptime(last_date, "%Y-%m-%d %H:%M:%S")

        thistab.load_this_data(first_date_range, last_date,)


        # MANAGE STREAM
        # date comes out stream in milliseconds
        stream_start_date = streams.Stream.define('Start_date',
                                                  start_date=first_date_range)()
        stream_end_date = streams.Stream.define('End_date', end_date=last_date)()

        stream_blockcount = streams.Stream.define('Blockcount', bcount=10)()


        # CREATE WIDGETS
        # create a slider widget

        initial_blockcount = 10
        blockcount_slider = Slider(start=0, end=1000, value=initial_blockcount,
                                   step=50, title='Blockcount',
                                   callback_policy="mouseup")
        datepicker_start = DatePicker(title="Start", min_date=first_date_range,
                                      max_date=last_date_range, value=first_date_range)
        datepicker_end = DatePicker(title="End", min_date=first_date_range,
                                    max_date=last_date_range, value=last_date)

        # declare plots
        dmap_hashrate = hv.DynamicMap(
            thistab.load_this_data, streams=[stream_start_date,
                                             stream_end_date,
                                             stream_blockcount],
                                             datashade=True) \
            .opts(plot=dict(width=1000, height=400))

        dmap_diff = hv.DynamicMap(
            thistab.difficulty_plot(),datashade=True)\
            .opts(plot=dict(width=1000, height=400))

        text_div = thistab.difficulty_text()

        # handle callbacks
        datepicker_start.on_change('value', update_start_date)
        datepicker_end.on_change('value', update_end_date)
        blockcount_slider.on_change("value", update_slider)


        # Render layout to bokeh server Document and attach callback
        renderer = hv.renderer('bokeh')
        hash_plot = renderer.get_plot(dmap_hashrate)
        diff_plot = renderer.get_plot(dmap_diff)

        # COMPOSE LAYOUT
        # put the controls in a single element
        controls = WidgetBox(
            datepicker_start, datepicker_end,
            blockcount_slider)


        # create the dashboard
        grid = gridplot([[controls],[hash_plot.state],
                         [diff_plot.state]])

        # Make a tab with the layout
        tab = Panel(child=grid, title='Hashrate')
        return tab

    except Exception:
        logger.error('rendering err:',exc_info=True)

        return tab_error_flag('hashrate')