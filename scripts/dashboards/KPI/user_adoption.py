import random

from holoviews import streams

from scripts.utils.mylogger import mylogger
from scripts.utils.myutils import tab_error_flag
from concurrent.futures import ThreadPoolExecutor
from tornado.locks import Lock
from scripts.utils.interfaces.KPI_interface import KPI
from config.dashboard import config as dashboard_config
from static.css.KPI_interface import KPI_card_css

from bokeh.layouts import gridplot, WidgetBox
from bokeh.models import Panel, Spacer
import gc
from bokeh.models.widgets import Div, \
    DatePicker, Select

from datetime import datetime, date

import holoviews as hv
from tornado.gen import coroutine
import numpy as np

lock = Lock()
executor = ThreadPoolExecutor()
logger = mylogger(__file__)

hv.extension('bokeh', logo=False)
renderer = hv.renderer('bokeh')


@coroutine
def KPI_user_adoption_tab(DAYS_TO_LOAD=90):
    class Thistab(KPI):
        def __init__(self, table, cols=[]):
            KPI.__init__(self, table,name='social_media',cols=cols)
            self.table = table
            self.df = None

            self.checkboxgroup = {
                'account_type': [],
                'update_type' : []
            }

            self.KPI_card_div = self.initialize_cards(self.page_width, height=350)

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
                'cards': self.section_header_div(text='Period to date:{}'.format(self.section_divider),
                                                 width=600, html_header='h2', margin_top=5,
                                                 margin_bottom=-155),
                'pop': self.section_header_div(text='Period over period:{}'.format(self.section_divider),
                                               width=600, html_header='h2', margin_top=5, margin_bottom=-155),
            }

            # ----------------------  DIVS ----------------------------

        def section_header_div(self, text, html_header='h2', width=600, margin_top=150, margin_bottom=-150):
            text = """<div style="margin-top:{}px;margin-bottom:-{}px;"><{} style="color:#4221cc;">{}</{}></div>""" \
                .format(margin_top, margin_bottom, html_header, text, html_header)
            return Div(text=text, width=width, height=15)


        # ----------------------  DIVS ----------------------------

        def reset_checkboxes(self, value='all',checkboxgroup=''):
            try:
                self.checkboxgroup[checkboxgroup].value = value
            except Exception:
                logger.error('reset checkboxes', exc_info=True)

        def information_div(self, width=400, height=300):
            div_style = """ 
                          style='width:350px;margin-right:-800px;
                          border:1px solid #ddd;border-radius:3px;background:#efefef50;' 
                      """
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

            """.format(div_style, self.header_style)
            div = Div(text=txt, width=width, height=height)
            return div

        # -------------------- CARDS -----------------------------------------
        def initialize_cards(self, width, height=250):
            try:
                txt = ''
                for period in ['year', 'quarter', 'month', 'week']:
                    design = random.choice(list(KPI_card_css.keys()))
                    txt += self.card(title='', data='', card_design=design)

                text = """<div style="margin-top:100px;display:flex; flex-direction:row;">
                       {}
                       </div>""".format(txt)
                div = Div(text=text, width=width, height=height)
                return div
            except Exception:
                logger.error('initialize cards', exc_info=True)

        # -------------------- GRAPHS -------------------------------------------

        def graph_periods_to_date(self,df1,filter_col):
            try:
                if self.account_type != 'all':
                    df1 = df1[df1.account_type == self.account_type]

                dct = {}
                for idx,period in enumerate(['week','month','quarter','year']):
                    df = self.period_to_date(df1, timestamp=dashboard_config['dates']['last_date'],
                                             timestamp_filter_col=filter_col, period=period)
                    # get unique instances
                    df = df[['address']]
                    df = df.compute()
                    df = df.drop_duplicates(keep='first')
                    #logger.warning('post duplicates dropped:%s', df.head(10))
                    data = len(df)
                    del df
                    gc.collect()
                    dct[period] = data
                self.update_cards(dct)

            except Exception:
                logger.error('graph periods to date',exc_info=True)



        def graph_period_over_period(self,period):
            try:
                periods = [period]
                start_date = self.pop_start_date
                end_date = self.pop_end_date
                if isinstance(start_date,date):
                    start_date = datetime.combine(start_date,datetime.min.time())
                if isinstance(end_date,date):
                    end_date = datetime.combine(end_date,datetime.min.time())
                cols = ['account_type', 'timestamp_of_first_event', 'day']
                df = self.load_df(start_date=start_date,end_date=end_date,cols=cols,
                                  timestamp_col='timestamp_of_first_event')
                if abs(start_date - end_date).days > 7:
                    if 'week' in periods:
                        periods.remove('week')
                if abs(start_date - end_date).days > 31:
                    if 'month' in periods:
                        periods.remove('month')
                if abs(start_date - end_date).days > 90:
                    if 'quarter' in periods:
                        periods.remove('quarter')

                if self.account_type != 'all':
                    df = df[df.account_type == self.account_type]

                # col for when list is empty
                self.variable = 'account_type'

                for idx,period in enumerate(periods):
                    df_period = self.period_over_period(df, start_date=start_date, end_date=end_date,
                                                        period=period, history_periods=self.pop_history_periods,
                                                        timestamp_col='timestamp_of_first_event')

                    groupby_cols = ['dayset','period']
                    if len(df_period) > 0:
                        df_period = df_period.groupby(groupby_cols).agg({'account_type':'count'})
                        df_period = df_period.reset_index()
                        df_period = df_period.compute()
                    else:
                        df_period = df_period.compute()
                        df_period = df_period.rename(index=str,columns={'day':'dayset'})
                    prestack_cols = list(df_period.columns)
                    logger.warning('Line 179:%s', df_period.head(10))
                    df_period = self.split_period_into_columns(df_period,col_to_split='period',value_to_copy='account_type')
                    logger.warning('line 180 df_period columns:%s',df_period.head(50))
                    poststack_cols = list(df_period.columns)
                    title = "{} over {}".format(period,period)


                    plotcols = list(np.setdiff1d(poststack_cols, prestack_cols))
                    df_period,plotcols = self.pop_include_zeros(df_period=df_period,plotcols=plotcols,period=period)
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
        thistab.graph_periods_to_date(thistab.df,'timestamp_of_first_event')
        thistab.section_header_updater('cards')
        thistab.section_header_updater('pop')
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        thistab.notification_updater("ready")

    def update_period_over_period(attrname, old, new):
        thistab.notification_updater("Calculations underway. Please be patient")
        thistab.pop_history_periods = history_periods_select.value
        thistab.pop_start_date=datepicker_period_start.value  # trigger period over period
        thistab.pop_end_date=datepicker_period_end.value  # trigger period
        thistab.trigger +=1
        stream_launch.event(launch=thistab.trigger)
        thistab.notification_updater("ready")


    def update_history_periods(attrname, old, new):
        thistab.notification_updater("Calculations underway. Please be patient")
        thistab.pop_history_periods = pop_number_select.value
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        thistab.notification_updater("ready")

    try:
        cols = ['address','account_type','update_type','balance','timestamp_of_first_event']
        thistab = Thistab(table='account_ext_warehouse', cols=cols)
        # -------------------------------------  SETUP   ----------------------------
        # format dates
        first_date_range = thistab.initial_date
        last_date_range = datetime.now().date()

        last_date = dashboard_config['dates']['last_date']
        first_date = datetime(last_date.year,1,1,0,0,0)

        thistab.df = thistab.load_df(first_date, last_date,cols,'timestamp_of_first_event')
        thistab.graph_periods_to_date(thistab.df,filter_col='timestamp_of_first_event')
        thistab.section_header_updater('cards')
        thistab.section_header_updater('pop')

        # MANAGE STREAM
        # date comes out stream in milliseconds
        # --------------------------------CREATE WIDGETS ---------------------------------
        datepicker_start = DatePicker(title="Start", min_date=first_date_range,
                                      max_date=last_date_range, value=first_date)
        datepicker_end = DatePicker(title="End", min_date=first_date_range,
                                    max_date=last_date_range, value=last_date)

        thistab.pop_end_date = last_date
        thistab.pop_start_date = thistab.first_date_in_period(thistab.pop_end_date, 'week')
        stream_launch = streams.Stream.define('Launch',launch=-1)()

        datepicker_period_start = DatePicker(title="Period start", min_date=first_date_range,
                                             max_date=last_date_range, value=thistab.pop_start_date)
        datepicker_period_end = DatePicker(title="Period end", min_date=first_date_range,
                                           max_date=last_date_range, value=thistab.pop_end_date)

        history_periods_select = Select(title='Select # of comparative periods',
                                        value='2',
                                        options=thistab.menus['history_periods'])
        account_type_select = Select(title='Select account type',
                                     value='all',
                                     options=thistab.menus['account_type'])
        pop_number_select = Select(title='Select # of comparative periods',
                                   value=str(thistab.pop_history_periods),
                                   options=thistab.menus['history_periods'])

        # ---------------------------------  GRAPHS ---------------------------
        hv_pop_week = hv.DynamicMap(thistab.pop_week, streams=[stream_launch])
        pop_week = renderer.get_plot(hv_pop_week)

        hv_pop_month = hv.DynamicMap(thistab.pop_month, streams=[stream_launch])
        pop_month = renderer.get_plot(hv_pop_month)

        hv_pop_quarter = hv.DynamicMap(thistab.pop_quarter, streams=[stream_launch])
        pop_quarter = renderer.get_plot(hv_pop_quarter)

        # -------------------------------- CALLBACKS ------------------------
        #datepicker_start.on_change('value', update)
        #datepicker_end.on_change('value', update)
        account_type_select.on_change('value', update_account)
        history_periods_select.on_change('value',update_period_over_period)
        datepicker_period_start.on_change('value',update_period_over_period)
        datepicker_period_end.on_change('value',update_period_over_period)
        pop_number_select.on_change('value',update_history_periods)

        # -----------------------------------LAYOUT ----------------------------
        # put the controls in a single element
        controls = WidgetBox(datepicker_start,datepicker_end, account_type_select)
        controls_pop = WidgetBox(datepicker_period_start,
                                 datepicker_period_end,
                                 history_periods_select)

        # create the dashboards
        grid = gridplot([
            [thistab.notification_div['top']],
            [Spacer(width=20, height=70)],
            [thistab.section_headers['cards']],
            [Spacer(width=20, height=2)],
            [thistab.KPI_card_div,controls],
            [thistab.section_headers['pop']],
            [Spacer(width=20, height=25)],
            [pop_week.state,controls_pop],
            [pop_month.state],
            [pop_quarter.state],
            [thistab.notification_div['bottom']]
        ])

        # Make a tab with the layout
        tab = Panel(child=grid, title='KPI: user adoption')
        return tab

    except Exception:
        logger.error('rendering err:', exc_info=True)
        return tab_error_flag('KPI accounts')


