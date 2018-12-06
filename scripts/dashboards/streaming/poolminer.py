import config
from scripts.utils.myutils import get_initial_blocks, tab_error_flag, ms_to_date
from scripts.utils.mylogger import mylogger
from scripts.utils.pythonCassandra import PythonCassandra


import datashader as ds
from bokeh.layouts import layout, column, row, gridplot, WidgetBox
from bokeh.models import ColumnDataSource, HoverTool, Panel, Range1d
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

        def __init__(self):
            if self.pc is None:
                self.pc = PythonCassandra()
                self.pc.createsession()
                self.pc.createkeyspace('aionv4')
                self.df = get_initial_blocks(self.pc)
                self.df1 = None
                self.n = 30
            else:
                pass

        def prep_dataset(self, start_date, end_date):
            try:
                logger.warning("prep dataset start date:%s", start_date)
                # change from milliseconds to seconds
                start_date = ms_to_date(start_date)
                end_date = ms_to_date(end_date)

                #set df1 while outputting bar graph
                self.df1 = self.df[(self.df.block_date >= start_date) &
                                   (self.df.block_date <= end_date)]

                self.df1 = self.df1.groupby('miner_addr').count()
                self.df1['percentage'] = 100*self.df1.block_number\
                                         /self.df1['block_number'].sum().compute()
                logger.warning('START DATE:%s',start_date)
                logger.warning('END DATE:%s',end_date)
                logger.warning('DF1:%s',self.df1.compute().head())
                #logger.warning('DF:%s',self.df.compute().head())

                return self.df1.hvplot.bar('miner_addr','block_number', rot=90,
                                           width=3000)
            except Exception:
                logger.error('munge df:', exc_info=True)


        def view_topN(self,n):
            # change n from string to int
            try:
                self.set_n(n)
                df2 = self.df1['percentage'].nlargest(self.n)
                df2 = df2.reset_index().compute()
                #bars_n = hv.Bars(df2, kdims=['miner_addr'],
                                 #vdims=['percentage'])
                title = 'Top {} miners'.format(self.n)
                bars_n = df2.hvplot.table(columns=['miner_addr','percentage'],
                                          title=title, width=300)

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



    def update_dates(attrname, old, new):
        # notify the holoviews stream of the slider update
        stream_dates.event(start_date=new[0],end_date=new[1])
        stream_topN.event(n=pm.n)



    def update_topN(attrname, old,new):
        stream_topN.event(n=new)


    try:
        # create class and get date range
        pm = Poolminer()
        ns = 1e-9

        # DYNAMIC DATES TO BOOKEND SLIDER
        df = pm.df.head(1)
        first_date = df['block_date'].values[0].astype(datetime)
        first_date = datetime.utcfromtimestamp(first_date * ns)
        first_date = pd.Timestamp(datetime.date(first_date))

        logger.warning('BLOCK FIRST DATE:%s', first_date)


        df = pm.df.tail(1)
        last_date = df['block_date'].values[-1].astype(datetime)
        last_date = datetime.utcfromtimestamp(last_date * ns)
        last_date = pd.Timestamp(datetime.date(last_date))
        logger.warning('LAST DATE:%s', last_date)

        '''   
        # STATIC DATES
        #format dates
        first_date = "2018-04-01 00:00:00"
        first_date = datetime.strptime(first_date, "%Y-%m-%d %H:%M:%S")
        last_date = datetime.now().date()
        '''

        # STREAMS Setup
        # date comes out stream in milliseconds
        stream_dates = streams.Stream.define('Dates', start_date=first_date,
                                             end_date=last_date)()
        stream_topN = streams.Stream.define('TopN', n='5')()

        # MANAGE
        millisecs_in_day = 86400000
        date_range_select = DateRangeSlider(title="Select Date Range ", start=first_date,
                                            end=last_date,
                                            value=(first_date, last_date),
                                            step=millisecs_in_day)

        # create a text widget for top N
        # text_input = TextInput(value='30', title="Top N Miners (Max 50):")
        topN_select = Select(title='Top N', value='5', options=menu)

        # add callbacks
        date_range_select.on_change('value', update_dates)
        topN_select.on_change("value", update_topN)


        renderer = hv.renderer('bokeh')


        # ALL MINERS
        dmap_all = hv.DynamicMap(pm.prep_dataset,
            streams=[stream_dates])\
            .opts(plot=dict(height=500, width=1000))
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