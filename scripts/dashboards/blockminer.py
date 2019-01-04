from concurrent.futures import ThreadPoolExecutor
from os.path import join, dirname

from bokeh.plotting import figure

from scripts.utils.mytab import DataLocation, Mytab
from scripts.utils.myutils import tab_error_flag, \
    ms_to_date, ns_to_date, set_params_to_load
from scripts.utils.mylogger import mylogger
from scripts.streaming.streamingDataframe import StreamingDataframe as SD
from config import dedup_cols

import datashader as ds
from bokeh.layouts import layout, column, row, gridplot, WidgetBox
from bokeh.models import CustomJS, ColumnDataSource, HoverTool, Panel, Button


import gc
from bokeh.models.widgets import DateRangeSlider, TextInput, Slider, Div, Select, \
    DatePicker, TableColumn, DataTable
from holoviews import streams
from holoviews.streams import Stream,RangeXY,RangeX,RangeY, Pipe
from pdb import set_trace
import hvplot.dask
import hvplot.pandas

from datetime import datetime, date, time

from tornado.gen import coroutine
executor = ThreadPoolExecutor(max_workers=5)


import holoviews as hv
hv.extension('bokeh',logo=False)
logger = mylogger(__file__)

menu = list()
for i in range(0, 400, 5):
    if i not in [0, 5]:
        menu.append(str(i))


@coroutine
def blockminer_tab():
    # source for top N table
    topN_src = ColumnDataSource(data=dict(percentage=[],
                                     miner_address=[],
                                     block_number=[]))


    class This_tab(Mytab):
        def __init__(self,table,cols,dedup_cols):
            Mytab.__init__(self, table, cols, dedup_cols)
            self.table = table
            self.df2 = None
            self.key_tab = 'blockminer'
            self.n = 20

        def load_this_data(self, start_date, end_date):
            end_date = datetime.combine(end_date, datetime.min.time())
            start_date = datetime.combine(start_date, datetime.min.time())

            logger.warning('load_data start date:%s', start_date)
            logger.warning('load_data end date:%s', end_date)

            self.df_load(start_date, end_date)

            return self.prep_dataset(start_date, end_date)


        def prep_dataset(self, start_date, end_date):
            try:
                logger.warning("prep dataset start date:%s", start_date)
                self.df1 = self.df1[['miner_address', 'block_number']]
                self.df1['miner_address'] = self.df1['miner_address']\
                    .map(self.poolname_verbose_trun)
                self.df1 = self.df1.groupby(['miner_address']).count()
                self.df1['percentage'] = 100*self.df1.block_number\
                                         /self.df1['block_number'].sum()
                self.df1 = self.df1.reset_index()
                logger.warning("topN column:%s",self.df1.columns.tolist())
                logger.warning('END prep dataset DF1:%s', self.df1.head())

                return self.df1.hvplot.bar('miner_address', 'block_number', rot=90,
                                           height= 600, width=1500, title='block_number by miner address',
                                           hover_cols=['percentage'])

            except Exception:
                logger.error('prep dataset:', exc_info=True)

        def view_topN(self):
            logger.warning("top n called:%s",self.n)
            # change n from string to int
            try:
                #table_n = df1.hvplot.table(columns=['miner_addr','percentage'],
                                          #title=title, width=400)
                logger.warning('top N:%s',self.n)
                df2 = self.df1.nlargest(self.n,'percentage')
                df2 = df2.compute()
                logger.warning('in view top n :%s',df2.head(10))

                new_data = dict(
                    percentage=df2.percentage.tolist(),
                    miner_address=df2.miner_address.tolist(),
                    block_number=df2.block_number.tolist()
                )
                topN_src.stream(new_data, rollover=self.n)
                columns = [
                    TableColumn(field="miner_address", title="Address"),
                    TableColumn(field="percentage", title="percentage"),
                    TableColumn(field="block_number", title="# of blocks")
                ]

                table_n = DataTable(source=topN_src, columns=columns, width=300, height=600)

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



    def update(attrname, old, new):
        notification_div.text = this_tab.notification_updater("Calculations underway."
                                                              " Please be patient")
        stream_start_date.event(start_date=datepicker_start.value)
        stream_end_date.event(end_date=datepicker_end.value)
        this_tab.set_n(topN_select.value)
        this_tab.view_topN()
        notification_div.text = this_tab.notification_updater("")

    # update based on selected top n
    def update_topN():
        notification_div.text = this_tab.notification_updater \
            ("Calculations in progress! Please wait.")
        logger.warning('topN selected value:%s',topN_select.value)
        this_tab.set_n(topN_select.value)
        this_tab.view_topN()
        notification_div.text = this_tab.notification_updater("")


    try:
        # create class and get date range
        cols = ['block_number', 'miner_address','block_timestamp', 'miner_addr', 'block_date', 'block_time']
        this_tab = This_tab('block',cols, dedup_cols)

        #STATIC DATES
        #format dates
        first_date_range = "2018-04-23 00:00:00"
        first_date_range = datetime.strptime(first_date_range, "%Y-%m-%d %H:%M:%S")
        last_date_range = datetime.now().date()
        first_date = datetime.strptime("2018-11-01",'%Y-%m-%d')
        last_date = datetime.now().date()

        notification_text = this_tab.notification_updater("")


        # STREAMS Setup
        # date comes out stream in milliseconds
        stream_start_date = streams.Stream.define('Start_date',
                                                  start_date=first_date)()
        stream_end_date = streams.Stream.define('End_date',
                                                end_date=last_date)()

        # create a text widget for top N
        topN_select = Select(title='Top N', value=str(this_tab.n), options=menu)

        datepicker_start = DatePicker(title="Start", min_date=first_date_range,
                                      max_date=last_date_range, value=first_date)
        datepicker_end = DatePicker(title="End", min_date=first_date_range,
                                    max_date=last_date_range, value=last_date)


        notification_div = Div(text=notification_text, width=500, height=50)

        # ALL MINERS
        # --------------------- ALL  MINERS ----------------------------------
        hv_bar_plot = hv.DynamicMap(this_tab.load_this_data,
                                    streams=[stream_start_date,
                                             stream_end_date],
                                    datashade=True)

        renderer = hv.renderer('bokeh')
        bar_plot = renderer.get_plot(hv_bar_plot)

        # --------------------- TOP N MINERS -----------------------------------
        # set up data source for the ton N miners table
        this_tab.view_topN()
        columns = [
            TableColumn(field="miner_address", title="Address"),
            TableColumn(field="percentage", title="percentage"),
            TableColumn(field="block_number", title="# of blocks")
        ]
        topN_table = DataTable(source=topN_src, columns=columns, width=400, height=600)

        # add callbacks
        datepicker_start.on_change('value', update)
        datepicker_end.on_change('value', update)
        topN_select.on_change("value", lambda attr, old, new: update_topN())


        download_button = Button(label='Save Table to CSV', button_type="success")
        download_button.callback = CustomJS(args=dict(source=topN_src),
            code=open(join(dirname(__file__),
                           "../../assets/js/topN_download.js")).read())


        # put the controls in a single element
        controls = WidgetBox(datepicker_start, datepicker_end,
                             download_button, topN_select)

        # create the dashboard
        grid = gridplot([[notification_div],
                         [controls, topN_table],
                         [bar_plot.state]])

        # Make a tab with the layout
        tab = Panel(child=grid, title='Blockminer')

        return tab

    except Exception:
        logger.error("Graph draw", exc_info=True)

        return tab_error_flag('blockminer')