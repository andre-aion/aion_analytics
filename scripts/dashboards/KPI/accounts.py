from os.path import join, dirname

from holoviews import streams

from scripts.utils.mylogger import mylogger
from scripts.utils.myutils import tab_error_flag, datetime_to_date
from scripts.utils.dashboards.mytab_interface import Mytab
from concurrent.futures import ThreadPoolExecutor
from tornado.locks import Lock
from scripts.utils.dashboards.KPI import KPI_interface

from bokeh.layouts import gridplot, WidgetBox
from bokeh.models import ColumnDataSource, Panel, CustomJS
import gc
from bokeh.models.widgets import Div, \
    DatePicker, TableColumn, DataTable, Button, Select, Paragraph

from datetime import datetime, timedelta

import holoviews as hv
from tornado.gen import coroutine

lock = Lock()
executor = ThreadPoolExecutor()
logger = mylogger(__file__)

hv.extension('bokeh', logo=False)

@coroutine
def KPIaccounts_tab(DAYS_TO_LOAD):
    class Thistab():
        def __init__(self, table, cols=[]):
            KPI_interface.__init__(self, table, cols)
            self.table = table
            self.df = None

        def title_div(self, text, width=700):
            text = '<h2 style="color:#4221cc;">{}</h2>'.format(text)
            return Div(text=text, width=width, height=15)

        def corr_information_div(self, width=400, height=300):
            txt = """
            <div {}>
            <h4 {}>How to interpret relationships </h4>
            <ul style='margin-top:-10px;'>
                <li>
                </li>
                <li>
                </li>
                <li>
                </li>
                <li>
                </li>
                 <li>
                </li>
                 <li>
                </li>
            </ul>
            </div>

            """.format(self.div_style, self.header_style)
            div = Div(text=txt, width=width, height=height)
            return div



    try:
        cols = ['block_timestamp','account_type','update_type','balance']
        thistab = Thistab(table='account_external_warehouse', cols=cols)
        # STATIC DATES
        # format dates
        first_date_range = "2019-01-23 00:00:00"
        first_date_range = datetime.strptime(first_date_range, "%Y-%m-%d %H:%M:%S")
        last_date_range = datetime.now().date()
        last_date = datetime.now().date()
        first_date = datetime_to_date(last_date - timedelta(days=DAYS_TO_LOAD))

        thistab.load_df(first_date, last_date)
        thistab.prep_data()

        # MANAGE STREAM
        # date comes out stream in milliseconds
        stream_launch = streams.Stream.define('Launch', launch=-1)()
        #stream_launch_matrix = streams.Stream.define('Launch_matrix', launch=-1)()
        #stream_launch_corr = streams.Stream.define('Launch_corr', launch=-1)()

        # CREATE WIDGETS
        datepicker_start = DatePicker(title="Start", min_date=first_date_range,
                                      max_date=last_date_range, value=first_date)
        datepicker_end = DatePicker(title="End", min_date=first_date_range,
                                    max_date=last_date_range, value=last_date)

        period_select = Select(title='Select aggregation period',
                               value='day',
                               options=thistab.menus['period'])
        event_select = Select(title='Select transfer type',
                              value='all',
                              options=thistab.menus['update_type'])

        # COMPOSE LAYOUT
        # put the controls in a single element
        controls_left = WidgetBox(
            datepicker_start,
            period_select)

        controls_right = WidgetBox(
            datepicker_end,
            event_select)

        # create the dashboards
        grid = gridplot([
            [thistab.notification_div],
            [controls_left, controls_right],
            [thistab.title_div('Relationships between variables', 400), variable_select],
            [corr_table.state, thistab.corr_information_div(), hist_plot.state],
            [matrix_plot.state]
        ])

        # Make a tab with the layout
        tab = Panel(child=grid, title='Account external activity')
        return tab

    except Exception:
        logger.error('rendering err:', exc_info=True)
        return tab_error_flag('KPI accounts')


