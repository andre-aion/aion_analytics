import random

from holoviews import streams

from scripts.utils.mylogger import mylogger
from scripts.utils.myutils import tab_error_flag
from concurrent.futures import ThreadPoolExecutor
from tornado.locks import Lock
from scripts.utils.interfaces.KPI_interface import KPI
from config.dashboard import config as dashboard_config

from bokeh.layouts import gridplot, WidgetBox
from bokeh.models import Panel, Button, Spacer
import gc
from bokeh.models.widgets import Div, \
    DatePicker, Select

from datetime import datetime, timedelta, date

import holoviews as hv
from tornado.gen import coroutine
import numpy as np
import pandas as pd

from static.css.KPI_interface import KPI_card_css

lock = Lock()
executor = ThreadPoolExecutor()
logger = mylogger(__file__)

hv.extension('bokeh', logo=False)
renderer = hv.renderer('bokeh')


@coroutine
def KPI_social_media_tab(panel_title,DAYS_TO_LOAD=90):
    class Thistab(KPI):
        def __init__(self, table,cols=[]):
            KPI.__init__(self, table,name='social_media',cols=cols)
            self.table = table
            self.df = None

            self.checkboxgroup = {}
            self.KPI_card_div = self.initialize_cards(self.page_width, height=350)
            self.ptd_startdate = datetime(datetime.today().year,1,1,0,0,0)


            self.timestamp_col = 'timestamp'
            self.social_media = self.menus['social_media'][0]
            self.crypto = 'aion'
            self.groupby_dict = {
                'watch': 'sum',
                'fork': 'sum',
                'issue': 'sum',
                'release': 'sum',
                'push': 'sum',
                'close': 'mean',
                'high': 'mean',
                'low': 'mean',
                'market_cap': 'mean',
                'volume': 'mean',
                'sp_close': 'mean',
                'sp_volume': 'mean',
                'russell_close': 'mean',
                'russell_volume': 'mean',
                'twu_tweets': 'sum',
                'twu_mentions': 'sum',
                'twu_positive': 'mean',
                'twu_compound': 'mean',
                'twu_neutral': 'mean',
                'twu_negative': 'mean',
                'twu_emojis_positive': 'mean',
                'twu_emojis_compound': 'mean',
                'twu_emojis_neutral': 'mean',
                'twu_emojis_negative': 'mean',
                'twu_emojis': 'sum',
                'twu_favorites': 'sum',
                'twu_retweets': 'sum',
                'twu_hashtags': 'sum',
                'twu_replies': 'sum',
                'twr_tweets': 'sum',
                'twr_mentions': 'sum',
                'twr_positive': 'mean',
                'twr_compound': 'mean',
                'twr_neutral': 'mean',
                'twr_negative': 'mean',
                'twr_emojis_positive': 'mean',
                'twr_emojis_compound': 'mean',
                'twr_emojis_neutral': 'mean',
                'twr_emojis_negative': 'mean',
                'twr_emojis': 'sum',
                'twr_favorites': 'sum',
                'twr_retweets': 'sum',
                'twr_hashtags': 'sum',
                'twr_replies': 'sum'
            }
            self.variables = sorted(list(self.groupby_dict.keys()))
            self.variable = self.variables[0]

            self.datepicker_pop_start = DatePicker(
                title="Period start", min_date=self.initial_date,
                max_date=dashboard_config['dates']['last_date'], value=dashboard_config['dates']['last_date'])
            # ------- DIVS setup begin
            self.page_width = 1200
            txt = """<hr/><div style="text-align:center;width:{}px;height:{}px;
                    position:relative;background:black;margin-bottom:200px">
                    <h1 style="color:#fff;margin-bottom:300px">{}</h1>
                    </div>""".format(self.page_width, 50, 'Welcome')
            self.notification_div = {
                'top': Div(text=txt, width=self.page_width, height=20),
                'bottom': Div(text=txt, width=self.page_width, height=10),
            }

            self.section_divider = '-----------------------------------'
            self.section_headers = {
                'cards': self.section_header_div(text='Period to date:{}'.format(
                    self.section_divider),
                    width=600, html_header='h2', margin_top=5,margin_bottom=-155),
                'pop': self.section_header_div(
                    text='Period over period:{}'.format(self.section_divider),
                    width=600, html_header='h2', margin_top=5, margin_bottom=-155),
            }

            # ----------------------  DIVS ----------------------------

        def section_header_div(self, text, html_header='h2', width=600,
                               margin_top=150, margin_bottom=-150):
            text = """<div style="margin-top:{}px;margin-bottom:-{}px;">
                <{} style="color:#4221cc;">{}</{}></div>""" \
                .format(margin_top, margin_bottom, html_header, text, html_header)
            return Div(text=text, width=width, height=15)

        def information_div(self, width=400, height=170):
            div_style = """ 
               style='width:350px;margin-right:-800px;
               border:1px solid #ddd;border-radius:3px;background:#efefef50;' 
           """
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

            """.format(div_style, self.header_style)
            div = Div(text=txt, width=width, height=height)
            return div

        def initialize_cards(self,width,height=250):
            try:
                txt = ''
                for period in ['year','quarter','month','week']:
                    design = random.choice(list(KPI_card_css.keys()))
                    txt += self.card(title='',data='',card_design=design)

                text = """<div style="margin-top:100px;display:flex; flex-direction:row;">
                {}
                </div>""".format(txt)
                div = Div(text=text, width=width, height=height)
                return div
            except Exception:
                logger.error('initialize cards', exc_info=True)



        # ------------------------- CARDS END -----------------------------------


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
                logger.warning('LINE 223:%s', df_current.head(15))
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
                dct = {}
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
                    dct[period] = data

                self.update_cards(dct)


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

                    logger.warning('LINE 368: dayset:%s',df_period.head(30))
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
                    # include current period is not extant
                    df_period, plotcols = self.pop_include_zeros(df_period,plotcols=plotcols, period=period)
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
        cols = []
        thistab = Thistab(table='external_daily', cols=cols)
        # -------------------------------------  SETUP   ----------------------------
        # format dates
        first_date_range = thistab.initial_date
        last_date_range = datetime.now().date()
        last_date = dashboard_config['dates']['last_date']
        first_date = datetime(last_date.year,1,1,0,0,0)

        loadcols = ['timestamp','crypto'] + thistab.variables
        loadcols = []
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
                                   value=str(5),
                                   options=thistab.menus['history_periods'])
        pop_button = Button(label="Select dates/periods, then click me!",width=15,button_type="success")

        variable_select = Select(title='Select variable', value=thistab.variable,
                                 options=thistab.variables)

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

        controls_pop = WidgetBox(thistab.datepicker_pop_start,datepicker_pop_end,pop_number_select,pop_button)
        controls_top = WidgetBox(social_media_select,crypto_select,variable_select)

        grid = gridplot([
            [thistab.notification_div['top']],
            [Spacer(width=20, height=70)],
            [thistab.information_div()],
            [thistab.section_headers['cards']],
            [Spacer(width=20, height=2)],
            [thistab.KPI_card_div,controls_top],
            [thistab.section_headers['pop']],
            [Spacer(width=20, height=25)],
            [pop_week.state,controls_pop],
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
