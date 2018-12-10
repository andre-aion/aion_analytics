import config
from scripts.utils.myutils import get_initial_blocks, tab_error_flag, \
    ms_to_date, ns_to_date, cass_load_from_daterange, check_loaded_dates, \
    construct_df_upon_load
from scripts.utils.mylogger import mylogger
from scripts.utils.pythonCassandra import PythonCassandra
from scripts.utils.pythonRedis import RedisStorage


import datashader as ds
from bokeh.layouts import layout, column, row, gridplot, WidgetBox
from bokeh.models import Plot, ColumnDataSource, HoverTool, Panel, Range1d
from bokeh.models.glyphs import HBar

import gc
from bokeh.io import curdoc
from bokeh.models.widgets import DateRangeSlider, TextInput, Slider, Div, Select
from holoviews import streams
from holoviews.streams import Stream,RangeXY,RangeX,RangeY, Pipe
from pdb import set_trace
import hvplot.dask
import hvplot.pandas

import holoviews as hv, param, dask.dataframe as dd
from holoviews.operation.datashader import rasterize, shade, datashade
from datetime import datetime, date
import numpy as np
import pandas as pd
import dask as dd

from tornado.gen import coroutine


import holoviews as hv
hv.extension('bokeh',logo=False)
logger = mylogger(__file__)

menu = list()
for i in range(0, 400, 5):
    if i != 0:
        menu.append(str(i))


@coroutine
def poolminer_tab():

    class Poolminer():
        pc = None
        querycols = ['block_number', 'miner_addr', 'block_date']
        table = 'block'
        def __init__(self):
            if self.pc is None:
                self.pc = PythonCassandra()
                self.pc.createsession()
                self.pc.createkeyspace('aionv4')
                self.df = None
                self.df1 = None
                self.n = 30
            else:
                pass


        def load_data(self, start_date, end_date):
            # find the boundaries of the loaded data, redis_data
            load_flags = check_loaded_dates(self.df, start_date,end_date)
            # load from redis, cassandra if necessary
            self.df = construct_df_upon_load(self.pc, self.df, self.table,
                                             self.querycols, start_date,
                                             end_date, load_flags)

            self.df1 = self.df.filter_dates(start_date, end_date)
            return self.prep_dataset(start_date, end_date)

        def filter_dates(self, start_date, end_date):
            # change from milliseconds to seconds
            start_date = ms_to_date(start_date)
            end_date = ms_to_date(end_date)

            # set df1 while outputting bar graph
            self.df1 = self.df[(self.df.block_date >= start_date) &
                               (self.df.block_date <= end_date)]


        def prep_dataset(self, start_date, end_date):
            try:

                logger.warning("prep dataset start date:%s", start_date)


                self.df1 = self.df1.groupby('miner_addr').count()
                self.df1['percentage'] = 100*self.df1.block_number\
                                         /self.df1['block_number'].sum().compute()
                logger.warning('START DATE:%s',start_date)
                logger.warning('END DATE:%s',end_date)
                logger.warning('DF1:%s',self.df1.compute().head())

                return self.df1.hvplot.bar('miner_addr','block_number', rot=90,
                                           width=1500)
            except Exception:
                logger.error('munge df:', exc_info=True)


        def view_topN(self, n):
            # change n from string to int
            try:
                self.set_n(n)
                df2 = self.df1['percentage'].nlargest(self.n)
                df2 = df2.reset_index().compute()
                title = 'Top {} miners'.format(self.n)
                bars_n = df2.hvplot.table(columns=['miner_addr','percentage'],
                                          title=title, width=400)

                del df2
                gc.collect()
                return bars_n
            except Exception:
                logger.error('view_topN:', exc_info=True)


        def set_n(self, n):
            if isinstance(n, int):
                pass
            else:
                try:
                    self.n = int(n)
                except Exception:
                    logger.error('set_n', exc_info=True)


    # notify the holoviews stream of the slider update
    def update_date_range(attrname, old, new):
        # notify the holoviews stream of the slider update
        stream_date_range.event(start_date=new[0], end_date=new[1])

    def update_topN(attrname, old,new):
        stream_topN.event(n=new)


    try:
        # create class and get date range
        pm = Poolminer()

        #STATIC DATES
        #format dates
        first_date_range = "2018-04-10 00:00:00"
        first_date_range = datetime.strptime(first_date_range, "%Y-%m-%d %H:%M:%S")
        last_date_range = datetime.now().date()
        last_date = "2018-09-30 00:00:00"
        last_date = datetime.strptime(last_date, "%Y-%m-%d %H:%M:%S")


        # STREAMS Setup
        # date comes out stream in milliseconds
        stream_date_range = streams.Stream.define('Dates', start_date=first_date_range,
                                                  end_date=last_date)()
        stream_topN = streams.Stream.define('TopN', n=str(pm.n))()

        # MANAGE
        millisecs_in_day = 86400000
        # SLIDER TO PULL DATA FROM CASSANDRA
        date_range_select = DateRangeSlider(title="Select Date Range ", start=first_date_range,
                                            end=last_date_range,
                                            value=(first_date_range, last_date),
                                            step=millisecs_in_day)

        # create a text widget for top N
        # text_input = TextInput(value='30', title="Top N Miners (Max 50):")
        topN_select = Select(title='Top N', value=str(pm.n), options=menu)

        # add callbacks
        date_range_select.on_change('value', update_date_range)
        topN_select.on_change("value", update_topN)


        renderer = hv.renderer('bokeh')


        # ALL MINERS
        dmap_all = hv.DynamicMap(pm.load_data,
                                 streams=[stream_date_range])\
            .opts(plot=dict(height=500, width=1500))
        all_plot = renderer.get_plot(dmap_all)

        # TOP N MINERS
        dmap_topN = hv.DynamicMap(pm.view_topN, streams=[stream_topN])\
            .opts(plot=dict(height=500, width=500))
        topN_plot=renderer.get_plot(dmap_topN)


        # put the controls in a single element
        controls = WidgetBox(date_range_select, topN_select)

        # create the dashboard
        grid = gridplot([[controls, topN_plot.state], [all_plot.state]])

        # Make a tab with the layout
        tab = Panel(child=grid, title='Poolminer')

        return tab

    except Exception:
        logger.error("poolminer:", exc_info=True)

        return tab_error_flag('poolminer')