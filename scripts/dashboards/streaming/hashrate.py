from scripts.utils.mylogger import mylogger
import config

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

from dask.distributed import Client
from dask import visualize, delayed

import holoviews as hv
import time

lock = Lock()
executor = ThreadPoolExecutor()
logger = mylogger(__file__)

hv.extension('bokeh', logo=False)

def hashrate_tab():
    @gen.coroutine
    def df():
        while True:
            logger.warning(config.df().tail())
            # Now the lock is released.
            time.sleep(2)


    executor.submit(df)


    def difficulty_text():    # Make a tab with the layout
        div = Div(text="""Welcome to Aion difficulty tab.""",
                  width=300, height=100)

        return div

    def difficulty_plot():
        try:
            p = config.df().hvplot.line(x='block_number',y='difficulty')
            return p
        except Exception:
            logger.error('plot errpr:',exc_info=True)

        return


    try:
        # declare plots
        dmap_diff = hv.DynamicMap(difficulty_plot(),datashade=True)\
            .opts(plot=dict(width=800, height=400))
        text_div = difficulty_text()


        # Render layout to bokeh server Document and attach callback
        renderer = hv.renderer('bokeh')
        diff_plot = renderer.get_plot(dmap_diff)

        # create the dashboard
        grid = gridplot([[text_div], [diff_plot.state]])

        # Make a tab with the layout
        tab = Panel(child=grid, title='Difficulty')
        return tab

    except Exception:
        logger.error('rendering err:',exc_info=True)

    return