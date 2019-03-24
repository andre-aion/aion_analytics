import random

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
import numpy as np
import pandas as pd

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

            self.section_headers = {
                'cards' : self.section_header_div('', 1400),
                'pop': self.section_header_div('', 1400) # period over period
            }

        # ----------------------  DIVS ----------------------------

        def reset_checkboxes(self, value='all',checkboxgroup=''):
            try:
                self.checkboxgroup[checkboxgroup].value = value
            except Exception:
                logger.error('reset checkboxes', exc_info=True)

        def section_header_div(self, text, width=1400):
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

        def card(self,title,count,card_design='folders',width=150,height=150):
            try:
                txt = """<div {}><h3>{}</h3></br>{}</div>""".format(self.KPI_card_css[card_design], title, count)
                div = Div(text=txt, width=width, height=height)
                return div

            except Exception:
                logger.error('card',exc_info=True)

        # -------------------- GRAPHS -------------------------------------------

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
                    del df
                    gc.collect()
                    title = "{} to date".format(period)

                    p = self.card(title=title, count=count, card_design=random.choice(list(self.KPI_card_css.keys())))
                    self.period_to_date_cards[period].text = p.text
                    #logger.warning('%s to date completed',period)

            except Exception:
                logger.error('graph periods to date',exc_info=True)


        def graph_period_over_period(self,launch=-1):
            try:
                start_date = self.period_start_date
                end_date = self.period_end_date
                cols = ['account_type', 'timestamp_of_first_event', 'day']
                timestamp_col = 'timestamp_of_first_event'
                periods = self.periods_to_plot.copy()
                df = self.load_df(start_date=start_date,end_date=end_date,cols=cols)
                if abs(start_date - end_date).days > 7:
                    if 'week' in periods:
                        periods.remove('week')
                if abs(start_date - end_date).days > 31:
                    if 'month' in periods:
                        periods.remove('month')
                if abs(start_date - end_date).days > 90:
                    if 'quarter' in periods:
                        periods.remove('quarter')


                for idx,period in enumerate(periods):
                    df_period = self.period_over_period(df, start_date = start_date, end_date=end_date,
                                                        period=period,history_periods=self.history_periods)
                    if self.account_type != 'all':
                        df_period = df_period[df_period.account_type == self.account_type]
                    groupby_cols = ['dayset','period']
                    df_period = df_period.groupby(groupby_cols).agg({'account_type':'count'})
                    df_period = df_period.reset_index()
                    prestack_cols = list(df_period.columns)
                    df_period = df_period.compute()
                    df_period = self.split_period_into_columns(df_period,col_to_split='period',value_to_copy='account_type')
                    poststack_cols = list(df_period.columns)

                    title = "{} over {}".format(period,period)
                    plotcols =list(np.setdiff1d(poststack_cols,prestack_cols))
                    logger.warning('line 147 cols to plot:%s',plotcols)
                    if idx == 0:
                        p = df_period.hvplot.bar('dayset',plotcols,rot=45,title=title,
                                                 stacked=False)
                    else:
                        p += df_period.hvplot.bar('dayset',plotcols,rot=45,title=title,
                                                  stacked=False)
                return p

            except Exception:
                logger.error('period over period to date', exc_info=True)

    def update_account(attrname, old, new):
        thistab.notification_updater("Calculations underway. Please be patient")
        thistab.account_type = new
        thistab.graph_periods_to_date(thistab.df)
        thistab.section_header_updater('cards')
        thistab.section_header_updater('pop')
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        thistab.notification_updater("ready")

    def update_period_over_period(attrname, old, new):
        thistab.notification_updater("Calculations underway. Please be patient")
        thistab.history_periods = history_periods_select.value
        thistab.period_start_date=datepicker_period_start.value  # trigger period over period
        thistab.period_end_date=datepicker_period_end.value  # trigger period
        thistab.trigger +=1
        stream_launch.event(launch=thistab.trigger)
        thistab.notification_updater("ready")

    try:
        cols = ['address','account_type','update_type','balance','timestamp_of_first_event']
        thistab = Thistab(table='account_external_warehouse', cols=cols)
        # -------------------------------------  SETUP   ----------------------------
        # format dates
        first_date_range = "2019-04-25 00:00:00"
        first_date_range = datetime.strptime(first_date_range, "%Y-%m-%d %H:%M:%S")
        last_date_range = datetime.now().date()
        last_date = datetime.now().date()
        first_date = datetime_to_date(datetime.strptime('2019-01-01 00:00:00', thistab.DATEFORMAT))

        thistab.df = thistab.load_df(first_date, last_date,cols,'timestamp_of_first_event')
        thistab.graph_periods_to_date(thistab.df)
        thistab.section_header_updater('cards')
        thistab.section_header_updater('pop')

        # MANAGE STREAM
        # date comes out stream in milliseconds
        # --------------------------------CREATE WIDGETS ---------------------------------
        datepicker_start = DatePicker(title="Start", min_date=first_date_range,
                                      max_date=last_date_range, value=first_date)
        datepicker_end = DatePicker(title="End", min_date=first_date_range,
                                    max_date=last_date_range, value=last_date)

        thistab.period_end_date = datetime_to_date(last_date - timedelta(days=3))
        thistab.period_start_date = datetime_to_date(last_date - timedelta(days=5))
        stream_launch = streams.Stream.define('Launch',launch=-1)()

        datepicker_period_start = DatePicker(title="Period start", min_date=first_date_range,
                                             max_date=last_date_range, value=thistab.period_start_date)
        datepicker_period_end = DatePicker(title="Period end", min_date=first_date_range,
                                           max_date=last_date_range, value=thistab.period_end_date)

        history_periods_select = Select(title='Select # of comparative periods',
                                        value='2',
                                        options=thistab.menus['history_periods'])
        account_type_select = Select(title='Select account type',
                                     value='all',
                                     options=thistab.menus['account_type'])

        # ---------------------------------  GRAPHS ---------------------------
        hv_period_over_period = hv.DynamicMap(thistab.graph_period_over_period,
                                              streams=[stream_launch])
        period_over_period = renderer.get_plot(hv_period_over_period)


        # -------------------------------- CALLBACKS ------------------------
        #datepicker_start.on_change('value', update)
        #datepicker_end.on_change('value', update)
        account_type_select.on_change('value', update_account)
        history_periods_select.on_change('value',update_period_over_period)
        datepicker_period_start.on_change('value',update_period_over_period)
        datepicker_period_end.on_change('value',update_period_over_period)


        # -----------------------------------LAYOUT ----------------------------
        # put the controls in a single element
        controls_left = WidgetBox(
            datepicker_start)

        controls_right = WidgetBox(
            datepicker_end,
            account_type_select)

        # create the dashboards
        grid = gridplot([
            [thistab.notification_div],
            [controls_left, controls_right],
            [thistab.section_headers['cards']],
            [thistab.period_to_date_cards['year'],thistab.period_to_date_cards['quarter'],
             thistab.period_to_date_cards['month'],thistab.period_to_date_cards['week']],
            [thistab.section_headers['pop']],
            [datepicker_period_start, datepicker_period_end,history_periods_select],
            [period_over_period.state]
        ])

        # Make a tab with the layout
        tab = Panel(child=grid, title='Account external activity')
        return tab

    except Exception:
        logger.error('rendering err:', exc_info=True)
        return tab_error_flag('KPI accounts')


