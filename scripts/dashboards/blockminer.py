from concurrent.futures import ThreadPoolExecutor
from os.path import join, dirname

from scripts.utils.mytab import DataLocation, Mytab
from scripts.utils.myutils import tab_error_flag, \
    ms_to_date, ns_to_date, set_params_to_load, \
    construct_df_upon_load
from scripts.utils.mylogger import mylogger
from scripts.streaming.streamingDataframe import StreamingDataframe as SD
from config import dedup_cols, columns as cols

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
    src = ColumnDataSource(data=dict(percentage=[],
                                     miner_addr=[],
                                     block_number=[]))

    class Thistab(Mytab):
        def __init__(self,table,cols,dedup_cols,query_cols=[]):
            Mytab.__init__(self, table, cols, dedup_cols, query_cols)
            self.table = table
            self.n = 20
            self.df2 = None

        def df_loaded_check(self, start_date, end_date):
            # check to see if block_tx_warehouse is loaded
            data_location = self.is_data_in_memory(start_date, end_date)
            if data_location == DataLocation.IN_MEMORY:
                # logger.warning('warehouse already loaded:%s', self.df.tail(40))
                pass
            elif data_location == DataLocation.IN_REDIS:
                self.load_data(start_date, end_date)
            else:
                # load the two tables and make the block
                self.load_data(start_date, end_date)
            self.filter_df(start_date, end_date)

        def load_this_data(self, start_date, end_date):
            end_date = datetime.combine(end_date, datetime.min.time())
            start_date = datetime.combine(start_date, datetime.min.time())

            logger.warning('load_data start date:%s', start_date)
            logger.warning('load_data end date:%s', end_date)

            self.df_loaded_check(start_date, end_date)

            # filter dates
            #logger.warning('load_data head:%s', self.df.head())
            #logger.warning('load_data tail:%s', self.df.tail())

            return self.prep_dataset(start_date, end_date)


        def prep_dataset(self, start_date, end_date):
            try:
                logger.warning("prep dataset start date:%s", start_date)
                self.df1 = self.df1[['miner_address', 'block_number']]
                self.df1['miner_address'] = self.df1['miner_address']\
                    .map(self.poolname_verbose_trun)
                self.df1 = self.df1.groupby(['miner_address']).count()
                self.df1['percentage'] = 100*self.df1.block_number\
                                         /self.df1['block_number'].sum().compute()
                logger.warning("topN column:%s",self.df1.columns.tolist())
                self.view_topN()
                #logger.warning('prep dataset DF1:%s', self.df1.compute().head())

                return self.df1.hvplot.bar('miner_address','block_number', rot=90,
                                           width=1500,title='block_number by miner address',
                                           hover_cols=['percentage'])
            except Exception:
                logger.error('munge df:', exc_info=True)

        def view_topN(self):
            logger.warning("top n called:%s",self.n)
            # change n from string to int
            try:
                #table_n = df1.hvplot.table(columns=['miner_addr','percentage'],
                                          #title=title, width=400)
                self.df1 = self.df1.reset_index() #
                self.df2 = self.df1.nlargest(self.n,'percentage')
                logger.warning('miner addr:%s',self.df2.miner_address.head(30))
                self.df2 = self.df2.compute()
                new_data = dict(
                    percentage=self.df2.percentage,
                    miner_addr=self.df2.miner_address,
                    block_number=self.df2.block_number
                )
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
        notification_div.text = pm.notification_updater \
            ("Calculations in progress! Please wait.")
        logger.warning('topN selected value:%s',topN_select.value)
        pm.set_n(topN_select.value)
        pm.view_topN()
        notification_div.text = pm.notification_updater("")


    try:
        # create class and get date range
        query_cols = ['block_number', 'miner_address', 'miner_addr', 'block_date', 'block_time']
        pm = Thistab('block',cols,dedup_cols)

        #STATIC DATES
        #format dates
        first_date_range = "2018-04-23 00:00:00"
        first_date_range = datetime.strptime(first_date_range, "%Y-%m-%d %H:%M:%S")
        last_date_range = datetime.now().date()
        first_date = datetime.strptime("2018-11-01",'%Y-%m-%d')
        last_date = datetime.now().date()

        notification_text = pm.notification_updater("")


        # STREAMS Setup
        # date comes out stream in milliseconds
        stream_start_date = streams.Stream.define('Start_date',
                                                  start_date=first_date)()
        stream_end_date = streams.Stream.define('End_date',
                                                  end_date=last_date)()

        stream_topN = streams.Stream.define('TopN', n=str(pm.n))()


        # create a text widget for top N
        topN_select = Select(title='Top N', value=str(pm.n), options=menu)

        datepicker_start = DatePicker(title="Start", min_date=first_date_range,
                                      max_date=last_date_range, value=first_date)
        datepicker_end = DatePicker(title="End", min_date=first_date_range,
                                    max_date=last_date_range, value=last_date)

        # Notification
        notification_div = Div(text=notification_text, width=500, height=50)

        # add callbacks
        datepicker_start.on_change('value', update_start_date)
        datepicker_end.on_change('value', update_end_date)
        topN_select.on_change("value", lambda attr, old, new: update_topN())

        renderer = hv.renderer('bokeh')

        # ALL MINERS
        dmap_all = hv.DynamicMap(pm.load_this_data,
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
                           "../../assets/js/topN_download.js")).read())

        # put the controls in a single element
        controls = WidgetBox(datepicker_start, datepicker_end,
                             download_button, topN_select)

        # create the dashboard
        grid = gridplot([[notification_div],
                         [controls, topN_table],
                         [all_plot.state]])

        # Make a tab with the layout
        tab = Panel(child=grid, title='Blockminer')

        return tab

    except Exception:
        logger.error("Graph draw", exc_info=True)

        return tab_error_flag('blockminer')