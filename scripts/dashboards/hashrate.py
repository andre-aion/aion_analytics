from scripts.utils.mylogger import mylogger
from scripts.utils.dashboard.hashrate import calc_hashrate
from scripts.utils.myutils import tab_error_flag
from scripts.utils.dashboard.mytab import Mytab
from config import dedup_cols
from concurrent.futures import ThreadPoolExecutor
from tornado.locks import Lock

from bokeh.layouts import gridplot, WidgetBox
from bokeh.models import Panel
from bokeh.models.widgets import Div, DatePicker, Select

from datetime import datetime
from holoviews import streams

import holoviews as hv
from tornado.gen import coroutine

lock = Lock()
executor = ThreadPoolExecutor()
logger = mylogger(__file__)

hv.extension('bokeh', logo=False)
table = 'block'

menu = [str(x) if x > 0 else '' for x in range(100,3000,100)]

@coroutine
def hashrate_tab():

    class _Thistab(Mytab):
        def __init__(self, table, hashrate_cols, dedup_cols):
            Mytab.__init__(self, table, hashrate_cols, dedup_cols)
            self.table = table
            self.key_tab = 'hashrate'
            self.blockcount = 100
            self.triggered_by_bcount = False

        def notification_updater(self, text):
            return '<h3  style="color:red">{}</h3>'.format(text)

        def load_this_data(self, start_date, end_date, bcount):
            end_date = datetime.combine(end_date, datetime.min.time())
            start_date = datetime.combine(start_date, datetime.min.time())
            if not self.triggered_by_bcount:
                self.df_load(start_date,end_date)
            if isinstance(bcount,str):
                self.blockcount = int(bcount)

            return self.hashrate_plot()

        def hashrate_plot(self):
            logger.warning("Hashrate plot started")
            try:
                df1 = calc_hashrate(self.df, self.blockcount)
                df1 = df1.compute()
                logger.warning("HASHRATE CALCS COMPLETED")
                #curve = hv.Curve(df, kdims=['block_number'], vdims=['hashrate'])\
                    #.options(width=1000,height=600)
                curve = df1.hvplot.line('block_number', 'hashrate',
                                        title='Hashrate',width=1000, height=600)
                logger.warning("HASHRATE GRAPH COMPLETED")

                return curve
            except Exception:
                logger.error('hashrate_plot:',exc_info=True)


        def difficulty_plot(self, start_date, end_date):
            try:

                logger.warning("Difficulty plot started")
                p = self.df.hvplot.line(x='block_number', y='difficulty',
                                        title='Difficulty')
                return p
            except Exception:
                logger.error('plot error:', exc_info=True)

    def update_start_date(attrname, old, new):
        # notify the holoviews stream of the slider update
        notification_div.text = thistab.notification_updater("Calculations underway."
                                                              " Please be patient")
        thistab.triggered_by_bcount = False
        thistab.blockcount = int(select_blockcount.value)
        stream_start_date.event(start_date=new)
        notification_div.text = thistab.notification_updater("")

    def update_end_date(attrname, old, new):
        # notify the holoviews stream of the slider update
        notification_div.text = thistab.notification_updater("Calculations underway."
                                                              " Please be patient")
        thistab.triggered_by_bcount = False
        thistab.blockcount = int(select_blockcount.value)
        stream_end_date.event(end_date=new)
        notification_div.text = thistab.notification_updater("")


    def update_blockcount():
        thistab.triggered_by_bcount = True
        # notify the holoviews stream of the slider update
        notification_div.text = thistab.notification_updater("Calculations underway."
                                                             " Please be patient")
        thistab.blockcount = int(select_blockcount.value)
        stream_blockcount.event(bcount=select_blockcount.value)
        notification_div.text = thistab.notification_updater("")
        thistab.triggered_by_bcount = False

    try:
        hashrate_cols=['block_time','block_timestamp','difficulty','block_number']
        thistab = _Thistab('block', hashrate_cols, dedup_cols)
        thistab.blockcount = 10

        # STATIC DATES
        # format dates
        first_date_range = "2018-04-23 00:00:00"
        first_date_range = datetime.strptime(first_date_range, "%Y-%m-%d %H:%M:%S")
        last_date_range = datetime.now().date()
        first_date = datetime.strptime("2018-12-15 00:00:00",'%Y-%m-%d %H:%M:%S')
        last_date = datetime.now().date()

        #thistab.load_this_data(first_date,last_date)
        notification_text = thistab.notification_updater("")
        #thistab.difficulty_plot()

        # MANAGE STREAM
        # date comes out stream in milliseconds
        stream_start_date = streams.Stream.define('Start_date',
                                                  start_date=first_date)()
        stream_end_date = streams.Stream.define('End_date', end_date=last_date)()
        stream_blockcount = streams.Stream.define('Blockcount',
                                                  bcount=str(thistab.blockcount))()

        notification_div = Div(text=notification_text, width=500, height=50)

        # CREATE WIDGETS
        # create a slider widget

        initial_blockcount = 100
        select_blockcount = Select(title='# of Blocks for mean(blocktime)',
                                   value=str(thistab.blockcount),
                                   options=menu)
        datepicker_start = DatePicker(title="Start", min_date=first_date_range,
                                      max_date=last_date_range, value=first_date)
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
            thistab.difficulty_plot,
            streams=[stream_start_date,
                     stream_end_date],
            datashade=True)\
            .opts(plot=dict(width=1000, height=400))

        # handle callbacks
        datepicker_start.on_change('value', update_start_date)
        datepicker_end.on_change('value', update_end_date)
        select_blockcount.on_change("value",lambda attr, old, new:update_blockcount())


        # Render layout to bokeh server Document and attach callback
        renderer = hv.renderer('bokeh')
        hash_plot = renderer.get_plot(dmap_hashrate)
        diff_plot = renderer.get_plot(dmap_diff)

        # COMPOSE LAYOUT
        # put the controls in a single element
        controls = WidgetBox(
            datepicker_start, datepicker_end,
            select_blockcount)


        # create the dashboard
        grid = gridplot([
            [notification_div],
            [controls],
            [hash_plot.state],
            [diff_plot.state]])

        # Make a tab with the layout
        tab = Panel(child=grid, title='Hashrate')
        return tab

    except Exception:
        logger.error('rendering err:',exc_info=True)

        return tab_error_flag('hashrate')