import config
from scripts.utils.myutils import get_initial_blocks, tab_error_flag
from scripts.utils.mylogger import mylogger
from scripts.utils.pythonCassandra import PythonCassandra


import datashader as ds
from bokeh.layouts import layout, column, row, gridplot, WidgetBox
from bokeh.models import ColumnDataSource, HoverTool, Panel, Range1d
import gc
from bokeh.io import curdoc
from bokeh.models.widgets import DateRangeSlider, TextInput, Slider, Div
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

from dask.distributed import Client
from dask import visualize, delayed

import holoviews as hv
hv.extension('bokeh',logo=False)
logger = mylogger(__file__)

def poolminer_tab():

    #make poolminer as singleton
    class Poolminer():
        '''
        df = None
        df1 = None
        n = 20
        pc = None
        '''

        class __impl:
            """ Implementation of the singleton interface """

            def spam(self):
                """ Test method, return singleton id """
                return id(self)

        # storage for the instance reference
        __instance = None


        def __init__(self):
            if Poolminer.__instance is None:
                # create and remember instance
                Poolminer.__instance = Poolminer.__impl()
                self.pc = PythonCassandra()
                self.pc.createsession()
                self.pc.createkeyspace('aionv4')
                self.df = get_initial_blocks(self.pc)
                self.df1 = None
                self.n = 30


            # Store instance reference as the only member in the handle
            self.__dict__['_Poolminer__instance'] = Poolminer.__instance

        def __getattr__(self, attr):
            """ Delegate access to implementation """
            return getattr(self.__instance, attr)

        def __setattr__(self, attr, value):
            """ Delegate access to implementation """
            return setattr(self.__instance, attr, value)


        def prep_dataset(self, start_date, end_date):
            try:
                #convert dates from timestamp to datetime
                if isinstance(start_date, int) == True:
                    # change from milliseconds to seconds
                    if start_date > 1630763200:
                        start_date = (start_date // 1000)
                    if end_date > 1630763200:
                        end_date = (end_date // 1000)

                    start_date = datetime.fromtimestamp(start_date)
                    end_date = datetime.fromtimestamp(end_date)

                #set df1 while outputting bar graph
                self.df1 = self.df[(self.df.block_date >= start_date) &
                              (self.df.block_date <= end_date)]

                self.df1 = self.df1.groupby('miner_addr').count().reset_index()
                self.df1['percentage'] = 100*self.df1.block_number\
                                         /self.df1['block_number'].sum().compute()
                print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
                print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
                logger.warning('START DATE:%s',start_date)
                logger.warning('PERCENTAGE:%s',self.df1.compute().head())
                logger.warning('END DATE:%s',end_date)
                print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
                print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')

                return hv.Bars(self.df1, hv.Dimension('miner_addr'),'block_number')
            except Exception:
                logger.error('munge df:', exc_info=True)


        def view_topN(self,n):
            # change n from string to int
            self.set_n(n)
            df2 = self.df1['percentage'].nlargest(self.n)
            df2 = df2.reset_index().compute()
            bars_n = hv.Bars(df2, kdims=['miner_addr'],
                             vdims=['percentage'])

            del df2
            gc.collect()
            return bars_n

        def set_n(self, n):
            if isinstance(n, int):
                pass
            else:
                self.n = int(n)

    def update_dates(attrname, old, new):
        # notify the holoviews stream of the slider update
        stream_dates.event(start_date=new[0],end_date=new[1])

    def update_topN(attrname, old,new):
        stream_topN.event(n=new)

    try:
        # STREAMS
        #format dates
        first_date = "2018-04-01 00:00:00"
        # multiply by 1000 to convert to milliseconds
        first_date = datetime.strptime(first_date, "%Y-%m-%d %H:%M:%S")
        last_date = datetime.now()

        stream_dates = streams.Stream.define('Dates',start_date = date(2018,4,1), end_date=last_date)()
        stream_topN = streams.Stream.define('TopN',n=30)()

        # MANAGE
        date_range_select = DateRangeSlider(title="Select Date Range ", start=first_date, end=last_date,
                                            value=(first_date, last_date), step=1)
        # create a text widget for top N
        text_input = TextInput(value='30', title="Top N Miners (Max 50):")

        # add callbacks
        date_range_select.on_change('value',update_dates)
        text_input.on_change("value", update_topN)

        # create plot and render to bokeh
        pm = Poolminer()

        renderer = hv.renderer('bokeh')


        # ALL MINERS
        dmap_all = hv.DynamicMap(pm.prep_dataset,
            streams=[stream_dates], datashade=True)\
            .opts(plot=dict(height=500, width=500))
        all_plot = renderer.get_plot(dmap_all)

        # TOP N MINERS
        #dmap_topN = hv.DynamicMap(pm.view_topN, streams=[stream_topN])\
            #.opts(plot=dict(height=500, width=500))
        #topN_plot=renderer.get_plot(dmap_topN)


        # put the controls in a single element
        controls = WidgetBox(date_range_select)

        # create the dashboard
        grid = gridplot([[controls], [all_plot.state]])

        # Make a tab with the layout
        tab = Panel(child=grid, title='Poolminer')

        return tab

    except Exception:
        logger.error("poolminer:",exc_info=True)

        return tab_error_flag('poolminer')