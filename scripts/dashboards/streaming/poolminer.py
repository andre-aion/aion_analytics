from os.path import join, dirname

import config
from scripts.utils.myutils import get_initial_blocks, tab_error_flag, \
    ms_to_date, ns_to_date, cass_load_from_daterange, set_params_to_load, \
    construct_df_upon_load
from scripts.utils.mylogger import mylogger
from scripts.utils.pythonCassandra import PythonCassandra
from scripts.streaming.streamingBlock import Block

import datashader as ds
from bokeh.layouts import layout, column, row, gridplot, WidgetBox
from bokeh.models import CustomJS, ColumnDataSource, HoverTool, Panel, Button
from bokeh.models.glyphs import HBar

import gc
from bokeh.io import curdoc
from bokeh.models.widgets import DateRangeSlider, TextInput, Slider, Div, Select, \
    DatePicker, TableColumn, DataTable
from holoviews import streams
from holoviews.streams import Stream,RangeXY,RangeX,RangeY, Pipe
from pdb import set_trace
import hvplot.dask
import hvplot.pandas

import holoviews as hv, param, dask.dataframe as dd
from holoviews.operation.datashader import rasterize, shade, datashade
from datetime import datetime, date, time
import numpy as np
import pandas as pd
import dask as dd

from tornado.gen import coroutine


import holoviews as hv
hv.extension('bokeh',logo=False)
logger = mylogger(__file__)

menu = list()
for i in range(0, 400, 5):
    if i not in [0, 5]:
        menu.append(str(i))


@coroutine
def poolminer_tab():
    # source for top N table
    src = ColumnDataSource(data=dict(percentage=[],
                                     miner_addr=[],
                                     block_number=[]))

    class Poolminer():
        pc = None
        querycols = ['block_number', 'miner_addr', 'block_date']
        table = 'block'
        block = Block()
        df = block.get_df()
        df1 = None
        n = 30

        def __init__(self):
            if self.pc is None:
                self.pc = PythonCassandra()
                self.pc.createsession()
                self.pc.createkeyspace('aionv4')
            else:
                pass


        def load_data(self, start_date, end_date):
            end_date = datetime.combine(end_date, datetime.min.time())
            start_date = datetime.combine(start_date, datetime.min.time())

            logger.warning('load_data start date:%s', start_date)
            logger.warning('load_data end date:%s', end_date)

            # find the boundaries of the loaded data, redis_data
            load_params = set_params_to_load(self.df, start_date, end_date)
            logger.warning('load_data:%s', load_params)
            # load from redis, cassandra if necessary
            self.df = construct_df_upon_load(self.pc, self.df, self.table,
                                             self.querycols, start_date,
                                             end_date, load_params)

            # filter dates
            logger.warning('load_data head:%s', self.df.head())
            logger.warning('load_data tail:%s', self.df.tail())
            self.filter_dates(start_date, end_date)

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

                self.view_topN()
                logger.warning('prep dataset START DATE:%s', start_date)
                logger.warning('prep dataset END DATE:%s', end_date)
                logger.warning('prep dataset DF1:%s', self.df1.compute().head())

                return self.df1.hvplot.bar('miner_addr','block_number', rot=90,
                                           width=1500,title='block_number by miner address',
                                           hover_cols=['percentage'])
            except Exception:
                logger.error('munge df:', exc_info=True)


        def view_topN(self):
            # change n from string to int
            try:
                #table_n = df1.hvplot.table(columns=['miner_addr','percentage'],
                                          #title=title, width=400)
                self.df2 = self.df1.nlargest(self.n,'percentage')
                self.df2 = self.df2.reset_index().compute()
                logger.warning('df2 after nlargest:%s',self.df2.head())
                new_data = dict(
                    percentage=self.df2.percentage,
                    miner_addr=self.df2.miner_addr,
                    block_number=self.df2.block_number
                )
                #src.stream
                src.stream(new_data, rollover=self.n)
                columns = [
                    TableColumn(field="miner_addr", title="Address"),
                    TableColumn(field="percentage", title="percentage"),
                    TableColumn(field="block_number", title="# of blocks")
                ]

                table_n = DataTable(source=src, columns=columns, width=300, height=600)

                gc.collect()
                return table_n
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


    # notify the holoviews stream of the slider updates
    def update_start_date(attrname, old, new):
        stream_start_date.event(start_date=new)

    def update_end_date(attrname, old, new):
        stream_end_date.event(end_date=new)

    # update based on selected top n
    def update_topN():
        logger.warning('topN selected value:%s',topN_select.value)
        logger.warning('topN singleton value:%s',pm.n)
        pm.set_n(topN_select.value)
        pm.view_topN()

    try:
        # create class and get date range
        pm = Poolminer()

        #STATIC DATES
        #format dates
        first_date_range = "2018-04-09 00:00:00"
        first_date_range = datetime.strptime(first_date_range, "%Y-%m-%d %H:%M:%S")
        last_date_range = datetime.now().date()
        last_date = "2018-05-30 00:00:00"
        last_date = datetime.strptime(last_date, "%Y-%m-%d %H:%M:%S")


        # STREAMS Setup
        # date comes out stream in milliseconds
        stream_start_date = streams.Stream.define('Start_date',
                                                  start_date=first_date_range)()
        stream_end_date = streams.Stream.define('End_date',
                                                  end_date=last_date)()

        stream_topN = streams.Stream.define('TopN', n=str(pm.n))()


        # create a text widget for top N
        topN_select = Select(title='Top N', value=str(pm.n), options=menu)

        datepicker_start = DatePicker(title="Start", min_date=first_date_range,
                                      max_date=last_date_range, value=first_date_range)
        datepicker_end = DatePicker(title="End", min_date=first_date_range,
                                    max_date=last_date_range, value=last_date)


        # add callbacks
        datepicker_start.on_change('value', update_start_date)
        datepicker_end.on_change('value', update_end_date)
        topN_select.on_change("value", lambda attr, old, new: update_topN())

        renderer = hv.renderer('bokeh')

        # ALL MINERS
        dmap_all = hv.DynamicMap(pm.load_data,
                                 streams=[stream_start_date, stream_end_date])\
            .opts(plot=dict(height=500, width=1500))
        all_plot = renderer.get_plot(dmap_all)

        # --------------------- TOP N MINERS -----------------------------------
        # set up data source for the ton N miners table

        columns = [
            TableColumn(field="miner_addr", title="Address"),
            TableColumn(field="percentage", title="percentage"),
            TableColumn(field="block_number", title="# of blocks")
        ]
        topN_table = DataTable(source=src, columns=columns, width=400, height=600)

        download_button = Button(label='Save Table to CSV', button_type="success")
        download_button.callback = CustomJS(args=dict(source=src),
            code=open(join(dirname(__file__),
                           "../../../assets/js/topN_download.js")).read())

        # put the controls in a single element
        controls = WidgetBox(datepicker_start, datepicker_end,
                             download_button, topN_select)

        # create the dashboard
        grid = gridplot([[controls, topN_table], [all_plot.state]])

        # Make a tab with the layout
        tab = Panel(child=grid, title='Poolminer')

        return tab

    except Exception:
        logger.error("poolminer:", exc_info=True)

        return tab_error_flag('poolminer')