from scripts.utils.mylogger import mylogger
from scripts.utils.hashrate import calc_hashrate, load_data
import config
from scripts.utils.pythonCassandra import PythonCassandra

from tornado import gen
from concurrent.futures import ThreadPoolExecutor
from tornado.locks import Lock


import datashader as ds
from bokeh.layouts import layout, column, row, gridplot, WidgetBox
from bokeh.models import ColumnDataSource, HoverTool, Panel, Range1d
import gc
from bokeh.io import curdoc
from bokeh.models.widgets import DateRangeSlider, TextInput, Slider, Div
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
from copy import copy
from holoviews import streams

from dask.distributed import Client
from dask import visualize, delayed

import holoviews as hv
import time

lock = Lock()
executor = ThreadPoolExecutor()
logger = mylogger(__file__)

hv.extension('bokeh', logo=False)

def hashrate_tab():
    pc = PythonCassandra()
    pc.createsession()
    pc.createkeyspace('aionv4')

    def get_initial_blocks():
        # convert to datetime
        mydate = ""
        to_check = tuple(range(0,4000))
        qry ="""SELECT block_number, difficulty, block_time FROM block WHERE
            block_number in """+str(to_check)
        df = pd.DataFrame(list(pc.session.execute(qry)))
        df = dd.dataframe.from_pandas(df,npartitions=10)
        return df

    '''
    @gen.coroutine
    def show_df():
        while True:
            logger.warning(config.df().tail())
            logger.warning('max_block_loaded:{}'
                           .format(config.max_block_loaded))
            # Now the lock is released.
            time.sleep(2)


    executor.submit(show_df)
    '''

    def hashrate_plot(bcount):
        df = get_initial_blocks()
        df = calc_hashrate(df, bcount).compute()
        curve = hv.Curve(df, kdims=['block_number'], vdims=['hashrate'])\
            .options(width=1000,height=600)
        del df
        gc.collect()
        return curve

    def update(attrname, old, new):
        # notify the holoviews stream of the slider update
        blockcount_stream.event(bcount=new)


    def difficulty_text():    # Make a tab with the layout
        div = Div(text="""Welcome to Aion difficulty tab.""",
                  width=300, height=100)

        return div

    def difficulty_plot():
        df = get_initial_blocks()
        try:
            p = df.hvplot.line(x='block_number',y='difficulty')
            return p
        except Exception:
            logger.error('plot errpr:',exc_info=True)

        return

    # MANAGE STREAM
    blockcount_stream = streams.Stream.define('Blockcount', bcount=10)()
    # create a slider widget



    initial_blockcount = 20
    blockcount_slider = Slider(start=100, end=1000, value=initial_blockcount,
                               step=100, title='Blockcount')


    try:

        dmap_hashrate = hv.DynamicMap(hashrate_plot,
                                      streams=[blockcount_stream],datashade=True)\
            .opts(plot=dict(width=800, height=400))


        # declare plots
        dmap_diff = hv.DynamicMap(difficulty_plot(),datashade=True)\
            .opts(plot=dict(width=800, height=400))
        text_div = difficulty_text()

        # handle controls
        blockcount_slider.on_change("value", update)

        # Render layout to bokeh server Document and attach callback
        renderer = hv.renderer('bokeh')
        diff_plot = renderer.get_plot(dmap_diff)
        hash_plot = renderer.get_plot(dmap_hashrate)

        # put the controls in a single element
        controls = WidgetBox(blockcount_slider)


        # create the dashboard
        grid = gridplot([[text_div,controls],[hash_plot.state],
                         [diff_plot.state]])

        # Make a tab with the layout
        tab = Panel(child=grid, title='Hashrate')
        return tab

    except Exception:
        logger.error('rendering err:',exc_info=True)

    return