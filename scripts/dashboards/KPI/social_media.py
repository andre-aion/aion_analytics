import random

from holoviews import streams

from scripts.utils.mylogger import mylogger
from scripts.utils.myutils import tab_error_flag, datetime_to_date, concat_dfs
from concurrent.futures import ThreadPoolExecutor
from tornado.locks import Lock
from scripts.utils.dashboards.KPI.KPI_interface import KPI
from config.dashboard import config as dashboard_config

from bokeh.layouts import gridplot, WidgetBox
from bokeh.models import Panel, Button
import gc
from bokeh.models.widgets import Div, \
    DatePicker, Select

from datetime import datetime, timedelta, date

import holoviews as hv
from tornado.gen import coroutine
import numpy as np
import pandas as pd

lock = Lock()
executor = ThreadPoolExecutor()
logger = mylogger(__file__)

hv.extension('bokeh', logo=False)
renderer = hv.renderer('bokeh')
variables = [
    'tw_mentions','tw_positive','tw_compound','tw_neutral',
    'tw_negative', 'tw_emojis_positive','tw_emojis_compound',
    'tw_emojis_negative','tw_emojis_count',
    'tw_replies_from_followers','tw_replies_from_following',
    'tw_reply_hashtags'
]

@coroutine
def KPI_social_media_tab(panel_title,DAYS_TO_LOAD=90):
    class Thistab(KPI):
        def __init__(self, table,cols=[]):
            KPI.__init__(self, table,name='social_media',cols=cols)
            self.table = table
            self.df = None
            txt = """<div style="text-align:center;background:black;width:100%;">
                      <h1 style="color:#fff;">
                      {}</h1></div>""".format('Welcome')
            self.notification_div = {
                'top': Div(text=txt, width=1400, height=20),
                'bottom': Div(text=txt, width=1400, height=10),
            }

            self.checkboxgroup = {}

            self.period_to_date_cards = {
                'year': self.card('',''),
                'quarter': self.card('', ''),
                'month': self.card('', ''),
                'week': self.card('', '')

            }
            self.ptd_startdate = datetime(datetime.today().year,1,1,0,0,0)

            self.section_headers = {
                'cards' : self.section_header_div('Period to date', 1400),
                'pop': self.section_header_div('Period over period-------------------', 600), # period over period
            }

            self.timestamp_col = 'timestamp'
            self.social_media = self.menus['social_media'][0]
            self.crypto = 'aion'
            self.variables = self.menus['social_media_variables']
            self.variable = self.variables[0]

            self.datepicker_pop_start = DatePicker(
                title="Period start", min_date=self.initial_date,
                max_date=dashboard_config['dates']['last_date'], value=dashboard_config['dates']['last_date'])


        # ----------------------  DIVS ----------------------------
        def section_header_div(self, text,html_header='h2',width=600):
            text = '<{} style="color:#4221cc;">{}</{}>'.format(html_header,text,html_header)
            return Div(text=text, width=width, height=15)

        def information_div(self, width=400, height=300):
            txt = """
            <div {}>
                <h4 {}>How to interpret sentiment score</h4>
                <ul style='margin-top:-10px;'>
                    <li>
                    Sentiment scores: positive, negative, neutral.
                    </li>
                    <li>
                    The sentiment scores are percentages.
                    </li>
                    <li>
                    The sentiment scores are averaged over the period.
                    </li>
                    <li>
                    (e.g.) Interpretation: over the quarter to date twitter comments
                    were 18% positive
                    </li>
                    
                </ul>
            </div>

            """.format(self.div_style, self.header_style)
            div = Div(text=txt, width=width, height=height)
            return div

        def card(self,title,data,card_design='folders',width=200,height=200):
            try:
                txt = """<div {}><h3>{}</h3></br>{}</div>""".format(self.KPI_card_css[card_design], title, data)
                div = Div(text=txt, width=width, height=height)
                return div

            except Exception:
                logger.error('card',exc_info=True)

        def period_to_date(self, df, timestamp=None, timestamp_filter_col=None, cols=[], period='week'):
            try:
                if timestamp is None:
                    timestamp = datetime.now()
                    timestamp = datetime(timestamp.year, timestamp.month, timestamp.day, timestamp.hour, 0, 0)

                start = self.first_date_in_period(timestamp, period)
                # filter

                df[timestamp_filter_col] = pd.to_datetime(df[timestamp_filter_col], format=self.DATEFORMAT_PTD)
                logger.warning('df:%s', df['timestamp'])

                df = df[(df[timestamp_filter_col] >= start) & (df[timestamp_filter_col] <= timestamp)]
                if len(cols) > 0:
                    df = df[cols]
                return df
            except Exception:
                logger.error('period to date', exc_info=True)

        def period_over_period(self, df, start_date, end_date, period,
                               history_periods=2, timestamp_col='timestamp_of_first_event'):
            try:
                # filter cols if necessary
                string = '0 {}(s) prev(current)'.format(period)

                # filter out the dates greater than today
                df_current = df.copy()
                df_current['period'] = string
                # label the days being compared with the same label
                df_current = self.label_dates_pop(df_current, period, timestamp_col)

                # zero out time information
                start = datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0)
                end = datetime(end_date.year, end_date.month, end_date.day, 0, 0, 0)

                cols = list(df.columns)
                counter = 1
                if isinstance(history_periods, str):
                    history_periods = int(history_periods)
                # make dataframes for request no. of periods
                start, end = self.shift_period_range(period, start, end)
                while counter < history_periods and start >= self.initial_date:
                    # load data
                    if period == 'quarter':
                        logger.warning('start:end %s:%s', start, end)
                    if self.crypto != 'all':
                        supplemental_where = "AND crypto = '{}'".format(self.crypto)
                    df_temp = self.load_df(start, end, cols, timestamp_col, supplemental_where=supplemental_where)
                    df_temp = df_temp.compute()
                    df_temp[timestamp_col] = pd.to_datetime(df_temp[timestamp_col])
                    if df_temp is not None:
                        if len(df_temp) > 1:
                            string = '{} {}(s) prev'.format(counter, period)
                            # label period
                            df_temp = df_temp.assign(period=string)
                            # relabel days to get matching day of week,doy, dom, for different periods
                            df_temp = self.label_dates_pop(df_temp, period, timestamp_col)
                            # logger.warning('df temp loaded for %s previous: %s',counter,len(df_temp))

                            df_current = pd.concat([df_current, df_temp])
                            del df_temp
                            gc.collect()
                    # shift the loading window
                    counter += 1
                    start, end = self.shift_period_range(period, start, end)
                return df_current
            except Exception:
                logger.error('period over period', exc_info=True)

            # label dates for period over period (pop)

        def label_dates_pop(self, df, period, timestamp_col):
            df[timestamp_col] = pd.to_datetime(df[timestamp_col])

            def label_qtr_pop(y):
                try:
                    curr_quarter = int((y.month - 1) / 3 + 1)
                    start = datetime(y.year, 3 * curr_quarter - 2, 1)
                    return abs((start - y).days)
                except Exception:
                    logger.error('df label quarter', exc_info=True)

            try:
                if period == 'week':
                    df['dayset'] = df[timestamp_col].dt.dayofweek
                elif period == 'month':
                    df['dayset'] = df[timestamp_col].dt.day
                elif period == 'year':
                    df['dayset'] = df[timestamp_col].timetuple().tm_yday
                elif period == 'quarter':
                    df['dayset'] = df[timestamp_col].apply(lambda x: label_qtr_pop(x))

                return df
            except Exception:
                logger.error('label data ', exc_info=True)

        # -------------------- GRAPHS -------------------------------------------
        def graph_periods_to_date(self, df1, timestamp_filter_col, variable):
            try:
                if self.crypto != 'all':
                    df1 = df1[df1.crypto == self.crypto]

                df1 = df1.compute()
                for idx, period in enumerate(['week', 'month', 'quarter', 'year']):
                    df = self.period_to_date(df1, timestamp=dashboard_config['dates']['last_date'],
                                             timestamp_filter_col=timestamp_filter_col, period=period)

                    # get unique instances
                    df = df[[variable]]
                    df = df.drop_duplicates(keep='first')
                    #logger.warning('post duplicates dropped:%s', df.head(10))
                    if self.groupby_dict[variable] == 'sum':
                        data = int(df[variable].sum())
                    elif self.groupby_dict[variable] == 'mean':
                        data = "{}%".format(round(df[variable].mean(),3))
                    del df
                    gc.collect()
                    title = "{} to date".format(period)

                    p = self.card(title=title, data=data, card_design=random.choice(list(self.KPI_card_css.keys())))
                    self.period_to_date_cards[period].text = p.text
                    logger.warning('%s to date completed',period)


            except Exception:
                logger.error('graph periods to date', exc_info=True)

        def graph_period_over_period(self, period):
            try:

                periods = [period]
                start_date = self.pop_start_date
                end_date = self.pop_end_date
                if isinstance(start_date, date):
                    start_date = datetime.combine(start_date, datetime.min.time())
                if isinstance(end_date, date):
                    end_date = datetime.combine(end_date, datetime.min.time())
                today = datetime.combine(datetime.today().date(), datetime.min.time())
                '''
                - if the start day is today (there is no data for today),
                  adjust start date
                '''
                if start_date == today:
                    logger.warning('START DATE of WEEK IS TODAY.!NO DATA DATA')
                    start_date = start_date - timedelta(days=7)
                    self.datepicker_pop_start.value = start_date

                cols = [self.variable, self.timestamp_col]
                supplemental_where = None
                if self.crypto != 'all':
                    supplemental_where = "AND crypto = '{}'".format(self.crypto)

                df = self.load_df(start_date=start_date, end_date=end_date, cols=cols,
                                  timestamp_col='timestamp',supplemental_where=supplemental_where)

                if abs(start_date - end_date).days > 7:
                    if 'week' in periods:
                        periods.remove('week')
                if abs(start_date - end_date).days > 31:
                    if 'month' in periods:
                        periods.remove('month')
                if abs(start_date - end_date).days > 90:
                    if 'quarter' in periods:
                        periods.remove('quarter')
                df = df.compute()
                for idx, period in enumerate(periods):
                    df_period = self.period_over_period(df, start_date=start_date, end_date=end_date,
                                                        period=period, history_periods=self.pop_history_periods,
                                                        timestamp_col='timestamp')

                    groupby_cols = ['dayset', 'period']
                    # logger.warning('line 150 df_period columns:%s',df.columns)
                    df_period = df_period.groupby(groupby_cols).agg({self.variable: 'sum'})
                    df_period = df_period.reset_index()
                    prestack_cols = list(df_period.columns)

                    df_period = self.split_period_into_columns(df_period, col_to_split='period',
                                                               value_to_copy=self.variable)

                    # short term fix: filter out the unnecessary first day added by a corrupt quarter functionality
                    if period == 'quarter':
                        min_day = df_period['dayset'].min()
                        logger.warning('LINE 252: MINIUMUM DAY:%s', min_day)
                        df_period = df_period[df_period['dayset'] > min_day]

                    poststack_cols = list(df_period.columns)

                    title = "{} over {}".format(period, period)
                    plotcols = list(np.setdiff1d(poststack_cols, prestack_cols))
                    # logger.warning('line 155 cols to plot:%s',plotcols)
                    if self.groupby_dict[self.variable] == 'sum':
                        xlabel = 'frequency'
                    elif self.groupby_dict[self.variable] == 'mean':
                        xlabel = '%'

                    if idx == 0:
                        p = df_period.hvplot.bar('dayset', plotcols, rot=45, title=title,
                                                 stacked=False,width=1200, height=400,value_label=xlabel)
                    else:
                        p += df_period.hvplot.bar('dayset', plotcols, rot=45, title=title,
                                                  stacked=False,width=1200, height=400,value_label=xlabel)
                return p

            except Exception:
                logger.error('period over period to date', exc_info=True)
    def update(attrname, old, new):
        thistab.notification_updater("Calculations underway. Please be patient")
        thistab.crypto = crypto_select.value
        thistab.variable = variable_select.value
        thistab.social_media = social_media_select.value
        thistab.graph_periods_to_date(thistab.df,'timestamp',thistab.variable)
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        thistab.notification_updater("ready")


    def update_period_over_period():
        thistab.notification_updater("Calculations underway. Please be patient")
        thistab.pop_history_periods = pop_number_select.value
        thistab.pop_start_date = thistab.datepicker_pop_start.value  # trigger period over period
        thistab.pop_end_date = datepicker_pop_end.value
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        thistab.notification_updater("ready")

    try:
        cols = ['timestamp','crypto'] + variables
        thistab = Thistab(table='external_daily', cols=cols)
        # -------------------------------------  SETUP   ----------------------------
        # format dates
        first_date_range = thistab.initial_date
        last_date_range = datetime.now().date()
        last_date = dashboard_config['dates']['last_date']
        first_date = datetime(last_date.year,1,1,0,0,0)

        loadcols = ['timestamp','crypto'] + thistab.variables
        thistab.df = thistab.load_df(first_date, last_date,loadcols,timestamp_col='timestamp')
        thistab.graph_periods_to_date(thistab.df,timestamp_filter_col='timestamp',variable=thistab.variable)
        thistab.section_header_updater('cards',label='')
        thistab.section_header_updater('pop',label='')

        # MANAGE STREAM
        # date comes out stream in milliseconds
        # --------------------------------CREATE WIDGETS ---------------------------------
        thistab.pop_end_date = last_date
        thistab.pop_start_date = thistab.first_date_in_period(thistab.pop_end_date, 'week')

        stream_launch = streams.Stream.define('Launch',launch=-1)()

        datepicker_pop_end = DatePicker(title="Period end", min_date=first_date_range,
                                        max_date=last_date_range, value=thistab.pop_end_date)

        pop_number_select = Select(title='Select # of comparative periods',
                                   value=str(thistab.pop_history_periods),
                                   options=thistab.menus['history_periods'])
        pop_button = Button(label="Select dates/periods, then click me!",width=15,button_type="success")

        variable_select = Select(title='Select variable', value=thistab.variable,
                                 options=thistab.menus['social_media_variables'])

        social_media_select = Select(title='Select social media',value=thistab.social_media,
                                     options=thistab.menus['social_media'])

        crypto_select = Select(title='Select cryptocurrency', value=thistab.crypto,
                               options=thistab.menus['cryptos'])

        # ---------------------------------  GRAPHS ---------------------------

        hv_pop_week = hv.DynamicMap(thistab.pop_week,streams=[stream_launch])
        pop_week = renderer.get_plot(hv_pop_week)

        hv_pop_month = hv.DynamicMap(thistab.pop_month,streams=[stream_launch])
        pop_month = renderer.get_plot(hv_pop_month)

        hv_pop_quarter = hv.DynamicMap(thistab.pop_quarter, streams=[stream_launch])
        pop_quarter = renderer.get_plot(hv_pop_quarter)


        # -------------------------------- CALLBACKS ------------------------

        variable_select.on_change('value', update)
        pop_button.on_click(update_period_over_period) # lags array
        social_media_select.on_change('value', update)
        crypto_select.on_change('value', update)


        # -----------------------------------LAYOUT ----------------------------
        # put the controls in a single element

        controls_pop_left = WidgetBox(thistab.datepicker_pop_start)
        controls_pop_centre = WidgetBox(datepicker_pop_end)
        controls_pop_right = WidgetBox(pop_button)

        grid = gridplot([
            [thistab.notification_div['top']],
            [social_media_select,crypto_select,variable_select],
            [thistab.section_headers['cards']],
            [thistab.period_to_date_cards['year'], thistab.period_to_date_cards['quarter'],
             thistab.period_to_date_cards['month'], thistab.period_to_date_cards['week'],
             thistab.information_div()],
            [thistab.section_headers['pop'], controls_pop_right],
            [controls_pop_left,controls_pop_centre,pop_number_select],
            [pop_week.state],
            [pop_month.state],
            [pop_quarter.state],
            [thistab.notification_div['bottom']]
        ])


        # Make a tab with the layout
        tab = Panel(child=grid, title=panel_title)
        return tab

    except Exception:
        logger.error('rendering err:', exc_info=True)
        return tab_error_flag(panel_title)
