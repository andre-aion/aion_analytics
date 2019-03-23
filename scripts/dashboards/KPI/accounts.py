from holoviews import streams

from scripts.utils.mylogger import mylogger
from scripts.utils.myutils import tab_error_flag, datetime_to_date
from concurrent.futures import ThreadPoolExecutor
from tornado.locks import Lock
from scripts.utils.dashboards.KPI.KPI_interface import KPI

from bokeh.layouts import gridplot, WidgetBox
from bokeh.models import Panel
import gc
from bokeh.models.widgets import Div, \
    DatePicker, Select

from datetime import datetime, timedelta

import holoviews as hv
from tornado.gen import coroutine

lock = Lock()
executor = ThreadPoolExecutor()
logger = mylogger(__file__)

hv.extension('bokeh', logo=False)
renderer = hv.renderer('bokeh')


@coroutine
def KPI_accounts_tab(DAYS_TO_LOAD=90):
    class Thistab(KPI):
        def __init__(self, table, cols=[]):
            KPI.__init__(self, table, cols)
            self.table = table
            self.df = None

            self.notification_div = Div(text=self.welcome_txt,width=1400,height=20)

            self.checkboxgroup = {
                'account_type': [],
                'update_type' : []
            }

            self.period_to_date_cards = {
                'year': self.card('',''),
                'quarter': self.card('', ''),
                'month': self.card('', ''),
                'week': self.card('', '')

            }

        def reset_checkboxes(self, value='all',checkboxgroup=''):
            try:
                self.checkboxgroup[checkboxgroup].value = value
            except Exception:
                logger.error('reset checkboxes', exc_info=True)

        def title_div(self, text, width=700):
            text = '<h2 style="color:#4221cc;">{}</h2>'.format(text)
            return Div(text=text, width=width, height=15)

        def information_div(self, width=400, height=300):
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

        def card(self,title,count,width=300,height=100):
            try:
                txt = """<div {}><h3>{}</h3></br>{}</div>""".format(self.KPI_css['circle'],title,count)
                div = Div(text=txt, width=width, height=height)
                return div

            except Exception:
                logger.error('card',exc_info=True)

        def graph_periods_to_date(self,df1):
            try:
                if self.account_type != 'all':
                    df1 = df1[df1.account_type == self.account_type]

                for idx,period in enumerate(['week','month','quarter','year']):
                    df = self.period_to_date(df1,filter_col='timestamp_of_first_event',period=period)
                    # get unique instances
                    df = df[['address']]
                    df = df.compute()
                    df = df.drop_duplicates(keep='first')
                    #logger.warning('post duplicates dropped:%s', df.head(10))
                    count = len(df)
                    logger.warning('LINE 94')
                    del df
                    gc.collect()
                    title = "{} to date".format(period)
                    p = self.card(title,count)
                    self.period_to_date_cards[period].text = p.text
                    logger.warning('%s to date completed',period)

            except Exception:
                logger.error('graph periods to date',exc_info=True)

        def graph_period_over_period(self,start_date, end_date,history_periods=1,
                                     periods=['month','week'],
                                     cols=['account_type','timestamp_of_first_event','year','month','day'],
                                     timestamp_col=None):
            try:
                df = self.load_df(start_date=start_date,end_date=end_date,cols=cols)
                for idx,period in enumerate(periods):
                    df_period = self.period_over_period(df, period=period,history_periods=history_periods)
                    if self.account_type != 'all':
                        df_period = df_period[df_period.account_type == self.account_type]
                    groupby_cols = ['year','month','day','period']
                    df_period = df_period.groupby(groupby_cols).agg({'account_type':'count'})
                    df_period = df_period.reset_index()
                    df_period = df_period.assign(date=df_period.month.map(str)+'-'+df_period.day.map(str))
                    title = "{} over {}".format(period,period)
                    if idx == 0:
                        p = df_period.hvplot.bar(x='date',y='account_type',rot=45,title=title)
                    else:
                        p += df_period.hvplot.bar(x='date',y='account_type',rot=45,title=title)
                return p

            except Exception:
                logger.error('period over period to date', exc_info=True)

    def update_account(attrname, old, new):
        thistab.notification_updater("Calculations underway. Please be patient")
        thistab.account_type = new
        thistab.graph_periods_to_date(thistab.df)
        stream_start_date_period.event(datepicker_period_start.value) # trigger period over period
        thistab.notification_updater("ready")

    def update_period_start(attrname, old, new):
        thistab.notification_updater("Calculations underway. Please be patient")
        stream_start_date_period.event(new)
        thistab.notification_updater("ready")

    def update_period_end(attrname, old, new):
        thistab.notification_updater("Calculations underway. Please be patient")
        stream_end_date_period.event(new)
        thistab.notification_updater("ready")


    try:
        cols = ['address','account_type','update_type','balance','timestamp_of_first_event']
        thistab = Thistab(table='account_external_warehouse', cols=cols)
        # STATIC DATES
        # format dates
        first_date_range = "2019-01-23 00:00:00"
        first_date_range = datetime.strptime(first_date_range, "%Y-%m-%d %H:%M:%S")
        last_date_range = datetime.now().date()
        last_date = datetime.now().date()
        first_date = datetime_to_date(last_date - timedelta(days=DAYS_TO_LOAD))

        thistab.df = thistab.load_df(first_date, last_date,cols,'timestamp_of_first_event')
        thistab.to_date_cards = thistab.graph_periods_to_date(thistab.df)

        # MANAGE STREAM
        # date comes out stream in milliseconds

        #stream_launch_matrix = streams.Stream.define('Launch_matrix', launch=-1)()
        #stream_launch_corr = streams.Stream.define('Launch_corr', launch=-1)()

        # CREATE WIDGETS
        datepicker_start = DatePicker(title="Start", min_date=first_date_range,
                                      max_date=last_date_range, value=first_date)
        datepicker_end = DatePicker(title="End", min_date=first_date_range,
                                    max_date=last_date_range, value=last_date)

        first_date = datetime_to_date(last_date - timedelta(days=7))
        stream_start_date_period = streams.Stream.define('Start_date',
                                                         start_date=first_date)()
        stream_end_date_period = streams.Stream.define('End_date',
                                                       end_date=last_date)()

        datepicker_period_start = DatePicker(title="Period start", min_date=first_date_range,
                                      max_date=last_date_range, value=first_date)
        datepicker_period_end = DatePicker(title="Period end", min_date=first_date_range,
                                    max_date=last_date_range, value=last_date)

        period_select = Select(title='Select aggregation period',
                               value='day',
                               options=thistab.menus['period'])
        account_type_select = Select(title='Select account type',
                              value='all',
                              options=thistab.menus['account_type'])

        # tables
        hv_period_over_period = hv.DynamicMap(thistab.graph_period_over_period,
                                              streams=[stream_start_date_period,
                                                       stream_end_date_period])
        period_over_period = renderer.get_plot(hv_period_over_period)

        # add callbacks
        #datepicker_start.on_change('value', update)
        #datepicker_end.on_change('value', update)
        datepicker_start.on_change('value', update_period_start)
        datepicker_end.on_change('value', update_period_end)
        account_type_select.on_change('value', update_account)

        # COMPOSE LAYOUT
        # put the controls in a single element
        controls_left = WidgetBox(
            datepicker_start,
            period_select)

        controls_right = WidgetBox(
            datepicker_end,
            account_type_select)

        # create the dashboards
        header_period_to_date = "KPI: New accounts to date"
        grid = gridplot([
            [thistab.notification_div],
            [controls_left, controls_right],
            [thistab.title_div(header_period_to_date, 400)],
            [thistab.period_to_date_cards['year'],thistab.period_to_date_cards['quarter'],
             thistab.period_to_date_cards['month'],thistab.period_to_date_cards['week']],
            [thistab.title_div('Period over period', 400)],
            [datepicker_period_start, datepicker_period_end],
            [period_over_period.state]
        ])

        # Make a tab with the layout
        tab = Panel(child=grid, title='Account external activity')
        return tab

    except Exception:
        logger.error('rendering err:', exc_info=True)
        return tab_error_flag('KPI accounts')


