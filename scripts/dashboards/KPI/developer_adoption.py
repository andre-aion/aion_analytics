import random

from holoviews import streams

from scripts.utils.mylogger import mylogger
from scripts.utils.myutils import tab_error_flag, datetime_to_date
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


@coroutine
def KPI_developer_adoption_tab(DAYS_TO_LOAD=90):
    class Thistab(KPI):
        def __init__(self, table,cols=[]):
            KPI.__init__(self, table,name='developer',cols=cols)
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
                'stat_sig':self.section_header_div('', html_header='h3',width=1000),
                'pop': self.section_header_div('', 1400) # period over period
            }

            self.timestamp_col = 'timestamp'
            self.variable = self.menus['developer_adoption_variables'][0]

            self.card_lists = self.set_grid_cards()

            self.datepicker_period_start = DatePicker(
                title="Period start", min_date=self.initial_date,
                max_date=datetime.now().date(), value=datetime.now().date())


        # ----------------------  DIVS ----------------------------
        def section_header_div(self, text,html_header='h2',width=1400):
            text = '<{} style="color:#4221cc;">{}</{}>'.format(html_header,text,html_header)
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

        def card(self,title,data,card_design='folders',width=200,height=200):
            try:
                txt = """<div {}><h3>{}</h3></br>{}</div>""".format(self.KPI_card_css[card_design], title, data)
                div = Div(text=txt, width=width, height=height)
                return div

            except Exception:
                logger.error('card',exc_info=True)


        def set_grid_cards(self):
            try:
                # make bunch of place holders
                card_lists = [[],[],[],[]]
                for key, i in self.card_grid_row.items():
                    for j in range(0,7):
                        card_lists[i].append(self.card('','',width=0,height=0))

                return card_lists

            except Exception:
                logger.error('graph periods to date', exc_info=True)

        def reset_grid_cards(self):
            try:
                # make bunch of place holders
                card_lists = [[],[],[],[]]
                for key, i in self.card_grid_row.items():
                    for j in range(0,7):
                        card_lists[i].append(self.card('','',width=0,height=0))

                return card_lists

            except Exception:
                logger.error('graph periods to date', exc_info=True)

        # -------------------- GRAPHS -------------------------------------------
        def graph_periods_to_date(self,df1,timestamp_filter_col,variable):
            try:
                self.card_lists = self.set_grid_cards()
                for idx,period in enumerate(['week','month','quarter','year']):
                    df = self.period_to_date(df1, timestamp=dashboard_config['dates']['last_date'],
                                             timestamp_filter_col=timestamp_filter_col, period=period)
                    # get unique instances
                    df = df.compute()
                    df = df.drop_duplicates(keep='first')

                    count = len(df)
                    gc.collect()

                    denom = df[variable].sum()
                    if denom != 0:
                        payroll_to_date = self.payroll_to_date(period)
                        cost_per_var = round(payroll_to_date/denom,2)
                        tmp_var = self.variable.split('_')
                        title = "{} to date".format(period)
                        title_cost = "${} per {}".format(cost_per_var,tmp_var[-1])

                        txt = """
                        <div {}>
                            <h3>{}</h3></br>{}
                            <h5>{}</h5></br>
                        </div>""".format(self.KPI_card_css[random.choice(list(self.KPI_card_css.keys()))],
                                         title, count,title_cost)

                    else:
                        title = "{} to date".format(period)
                        txt = """
                        <div {}>
                            <h3>{}</h3></br>{}
                        </div>""".format(
                            self.KPI_card_css[random.choice(list(self.KPI_card_css.keys()))],
                            title, count)


                    #self.period_to_date_cards[period].text = p.text
                    card_position_counter = 0
                    i = self.card_grid_row[period]
                    self.card_lists[i][card_position_counter].text = txt
                    self.card_lists[i][card_position_counter].width = 200
                    self.card_lists[i][card_position_counter].height = 200

                    # add the statistically significant point estimates
                    self.calc_sig_effect_card_data(df,variable_of_interest=self.variable, period=period)
                    '''
                    logger.warning('self.sig_effect_dict:%s',self.sig_effect_dict)
                    for var in self.sig_effect_dict.keys():
                        card_position_counter += 1
                        p = self.card(
                            title=self.sig_effect_dict[var]['title'],
                            data=self.sig_effect_dict[var]['point_estimate'],
                            card_design=random.choice(list(self.KPI_card_css.keys())))
                        self.card_lists[i][card_position_counter].text = p.text
                        self.card_lists[i][card_position_counter].width = 200
                        self.card_lists[i][card_position_counter].height = 200

                    #logger.warning('%s to date completed',period)
                    '''
                    del df

            except Exception:
                logger.error('graph periods to date',exc_info=True)


        def graph_period_over_period(self,period):
            try:
                periods = [period]
                start_date = self.period_start_date
                end_date = self.period_end_date
                if isinstance(start_date,date):
                    start_date = datetime.combine(start_date,datetime.min.time())
                if isinstance(end_date,date):
                    end_date = datetime.combine(end_date,datetime.min.time())
                today = datetime.combine(datetime.today().date(),datetime.min.time())
                '''
                - if the start day is today (there is no data for today),
                  adjust start date
                '''
                if start_date == today:
                    logger.warning('START DATE IS TODAY.!NO DATA DATA')
                    start_date = start_date - timedelta(days=7)
                    self.datepicker_period_start.value = start_date

                cols = [self.variable,self.timestamp_col, 'day']
                df = self.load_df(start_date=start_date,end_date=end_date,cols=cols,timestamp_col='timestamp')
                if abs(start_date - end_date).days > 7:
                    if 'week' in periods:
                        periods.remove('week')
                if abs(start_date - end_date).days > 31:
                    if 'month' in periods:
                        periods.remove('month')

                for idx,period in enumerate(periods):
                    df_period = self.period_over_period(df, start_date = start_date, end_date=end_date,
                                                        period=period,history_periods=self.history_periods,
                                                        timestamp_col='timestamp')

                    groupby_cols = ['dayset','period']
                    #logger.warning('line 150 df_period columns:%s',df.columns)
                    df_period = df_period.groupby(groupby_cols).agg({self.variable:'sum'})
                    df_period = df_period.reset_index()
                    prestack_cols = list(df_period.columns)
                    df_period = df_period.compute()
                    df_period = self.split_period_into_columns(df_period,col_to_split='period',
                                                               value_to_copy=self.variable)
                    poststack_cols = list(df_period.columns)

                    title = "{} over {}".format(period,period)
                    plotcols =list(np.setdiff1d(poststack_cols,prestack_cols))
                    #logger.warning('line 155 cols to plot:%s',plotcols)
                    if idx == 0:
                        p = df_period.hvplot.bar('dayset',plotcols,rot=45,title=title,
                                                 stacked=False)
                    else:
                        p += df_period.hvplot.bar('dayset',plotcols,rot=45,title=title,
                                                  stacked=False)
                return p

            except Exception:
                logger.error('period over period to date', exc_info=True)

        def pop_week(self,launch=-1):
            try:
                return self.graph_period_over_period('week')
            except Exception:
                logger.error('pop week', exc_info=True)

        def pop_month(self, launch=-1):
            try:
                return self.graph_period_over_period('month')
            except Exception:
                logger.error('pop week', exc_info=True)

        def pop_quarter(self, launch=-1):
            try:
                return self.graph_period_over_period('quarter')
            except Exception:
                logger.error('pop week', exc_info=True)

    def update(attrname, old, new):
        thistab.notification_updater("Calculations underway. Please be patient")
        thistab.variable = variable_select.value
        thistab.ptd_startdate = datepicker_ptd_start.value
        today = datetime.today()
        end_date = datetime(today.year,today.month,today.day,0,0,0)
        thistab.df = thistab.load_df(thistab.ptd_startdate, end_date,cols,'timestamp')
        thistab.graph_periods_to_date(thistab.df,'timestamp',thistab.variable)
        thistab.section_header_updater('cards')
        thistab.section_header_updater('pop')
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        thistab.notification_updater("ready")

    def update_period_over_period():
        thistab.notification_updater("Calculations underway. Please be patient")
        thistab.history_periods = history_periods_select.value
        thistab.period_start_date = thistab.datepicker_period_start.value  # trigger period over period
        thistab.period_end_date = datepicker_period_end.value
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        thistab.notification_updater("ready")

    try:
        cols = ['aion_fork','aion_watch','aion_release','aion_issue','aion_push','timestamp']
        thistab = Thistab(table='account_ext_warehouse', cols=cols)
        # -------------------------------------  SETUP   ----------------------------
        # format dates
        first_date_range = thistab.initial_date
        last_date_range = datetime.now().date()
        last_date = dashboard_config['dates']['last_date']
        first_date = datetime(last_date.year,1,1,0,0,0)

        thistab.df = thistab.load_df(first_date, last_date,cols,'timestamp')
        thistab.graph_periods_to_date(thistab.df,timestamp_filter_col='timestamp',variable=thistab.variable)
        thistab.section_header_updater('cards')
        thistab.section_header_updater('pop')

        # MANAGE STREAM
        # date comes out stream in milliseconds
        # --------------------------------CREATE WIDGETS ---------------------------------
        datepicker_ptd_start = DatePicker(title="First date", min_date=first_date_range,
                                      max_date=last_date_range, value=first_date)


        thistab.period_end_date = last_date
        thistab.period_start_date = thistab.first_date_in_period(thistab.period_end_date,'week')

        stream_launch = streams.Stream.define('Launch',launch=-1)()


        datepicker_period_end = DatePicker(title="Period end", min_date=first_date_range,
                                           max_date=last_date_range, value=thistab.period_end_date)

        history_periods_select = Select(title='Select # of comparative periods',
                                        value='2',
                                        options=thistab.menus['history_periods'])
        variable_select = Select(title='Select variable',
                                     value=thistab.variable,
                                     options=thistab.menus['developer_adoption_variables'])
        period_over_period_button = Button(label="Select dates/periods, then click me!",width=15,button_type="success")


        # ---------------------------------  GRAPHS ---------------------------
        hv_pop_week = hv.DynamicMap(thistab.pop_week,streams=[stream_launch])
        pop_week = renderer.get_plot(hv_pop_week)

        hv_pop_month = hv.DynamicMap(thistab.pop_month,streams=[stream_launch])
        pop_month = renderer.get_plot(hv_pop_month)

        hv_pop_quarter = hv.DynamicMap(thistab.pop_quarter, streams=[stream_launch])
        pop_quarter = renderer.get_plot(hv_pop_quarter)


        # -------------------------------- CALLBACKS ------------------------

        variable_select.on_change('value', update)
        period_over_period_button.on_click(update_period_over_period) # lags array
        datepicker_ptd_start.on_change('value',update)

        # -----------------------------------LAYOUT ----------------------------
        # put the controls in a single element
        controls_left = WidgetBox(datepicker_ptd_start)
        controls_right = WidgetBox(variable_select)


        grid_before = [ [thistab.notification_div['top']],
            [controls_left, controls_right],
            [thistab.section_headers['cards']]]

        grid_after = [[thistab.section_headers['pop'],period_over_period_button],
            [thistab.datepicker_period_start, history_periods_select],
            [pop_week.state],
            [pop_month.state],
            [pop_quarter.state],
            [thistab.notification_div['bottom']]]

        grid_data = grid_before + thistab.card_lists + grid_after
        grid = gridplot(grid_data)

        # Make a tab with the layout
        tab = Panel(child=grid, title='KPI: developer adoption')
        return tab

    except Exception:
        logger.error('rendering err:', exc_info=True)
        return tab_error_flag('KPI: developer adoption')
