import random

from holoviews import streams

from scripts.databases.pythonMongo import PythonMongo
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


@coroutine
def KPI_projects_tab(panel_title, DAYS_TO_LOAD=90):
    class Thistab(KPI):
        def __init__(self, table, cols=[]):
            KPI.__init__(self, table, name='project', cols=cols)
            self.table = table
            self.df = None
            self.df_pop = None
            txt = """<div style="text-align:center;background:black;width:100%;">
                      <h1 style="color:#fff;">
                      {}</h1></div>""".format('Welcome')
            self.notification_div = {
                'top': Div(text=txt, width=1400, height=20),
                'bottom': Div(text=txt, width=1400, height=10),
            }

            self.checkboxgroup = {}

            self.period_to_date_cards = {
                'year': self.card('', ''),
                'quarter': self.card('', ''),
                'month': self.card('', ''),
                'week': self.card('', '')

            }
            self.ptd_startdate = datetime(datetime.today().year, 1, 1, 0, 0, 0)

            self.timestamp_col = 'project_startdate_actual'
            self.pym = PythonMongo('aion')
            self.groupby_dict = {
                'project': 'sum',
                'project_duration': 'sum',
                'project_start_delay': 'mean',
                'project_end_delay': ' mean',

                'milestone': 'sum',
                'milestone_duration': 'sum',
                'milestone_start_delay': 'mean',
                'milestone_end_delay': ' mean',

                'task': 'sum',
                'task_duration': 'sum',
                'task_start_delay': 'mean',
                'task_end_delay': ' mean',
            }
            
            self.menus = {
                'status' : ['all','open','closed'],
                'type':['all','research','reconciliation','audit','innovation',
                        'construction','manufacturing','conference'],
                'gender':['all','male','female'],
                'variables':list(self.groupby_dict.keys()),
                'history_periods': ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'],
            }

            self.status = 'all'
            self.pm_gender = 'all'
            self.m_gender = 'all'
            self.t_gender = 'all'
            self.type = 'all'
            self.variables = sorted(list(self.groupby_dict.keys()))
            self.variable = self.variables[0]
            self.groupby_var = 'project'


            self.section_headers = {
                'cards': self.section_header_div(text='',width=1000),
                'pop': self.section_header_div(text='Period over period:----------------------------------', width=600)
            }


        # ----------------UTILS ---------------------

        # ----------------------  DIVS ----------------------------
        def section_header_div(self, text, html_header='h2', width=600):
            text = '<{} style="color:#4221cc;">{}</{}>'.format(html_header, text, html_header)
            return Div(text=text, width=width, height=15)

        def information_div(self, width=400, height=300):
            txt = """
            <div {}>
                <h4 {}>How to interpret sentiment score</h4>
                <ul style='margin-top:-10px;'>
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

        def card(self, title, data, card_design='folders', width=200, height=200):
            try:
                txt = """<div {}><h3>{}</h3></br>{}</div>""".format(self.KPI_card_css[card_design], title, data)
                div = Div(text=txt, width=width, height=height)
                return div

            except Exception:
                logger.error('card', exc_info=True)

        def load_df(self,req_startdate, req_enddate, table, cols, timestamp_col):
            try:
                # get min and max of loaded df
                if self.df is not None:
                    loaded_min = self.df[timestamp_col].min()
                    loaded_max = self.df[timestamp_col].max()

                    if loaded_min <= req_startdate and loaded_max >= req_enddate:
                        df = self.df[(self.df[timestamp_col] >= req_startdate) &
                                     (self.df[timestamp_col] <= req_enddate)]
                        return df
                return self.pym.load_df(req_startdate, req_enddate, table=table,
                                      cols=cols, timestamp_col=timestamp_col)

            except Exception:
                logger.error('load_df', exc_info=True)

        def filter_df(self, df1):
            if self.status != 'all':
                df1 = df1[df1.status == self.status]
            if self.pm_gender != 'all':
                df1 = df1[df1.project_owner_gender == self.pm_gender]
            if self.m_gender != 'all':
                df1 = df1[df1.milestone_owner_gender == self.m_gender]
            if self.t_gender != 'all':
                df1 = df1[df1.task_owner_gender == self.t_gender]

            if self.type != 'all':
                df1 = df1[df1.type == self.type]
            return df1

        def period_to_date(self, df, timestamp=None, timestamp_filter_col=None, cols=[], period='week'):
            try:
                if timestamp is None:
                    timestamp = datetime.now()
                    timestamp = datetime(timestamp.year, timestamp.month, timestamp.day, timestamp.hour, 0, 0)

                start = self.first_date_in_period(timestamp, period)
                # filter

                df[timestamp_filter_col] = pd.to_datetime(df[timestamp_filter_col], format=self.DATEFORMAT_PTD)
                #logger.warning('df:%s', df[self.timestamp_col])

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
                df_current = self.filter_df(df_current)
                df_current['period'] = string
                # label the days being compared with the same label
                df_current = self.label_dates_pop(df_current, period, timestamp_col)
                cols = [self.variable,'period','dayset']
                if 'project' in self.variable:
                    if self.variable != 'project':
                        df_current = df_current[[self.variable,'period','dayset','project']]
                elif 'milestone' in self.variable:
                    if self.variable != 'milestone':
                        df_current = df_current[[self.variable,'period','dayset','milestone','project']]
                elif 'task' in self.variable:
                    if self.variable != 'task':
                        df_current = df_current[[self.variable,'period','dayset','task','milestone','project']]

                # zero out time information
                start = datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0)
                end = datetime(end_date.year, end_date.month, end_date.day, 0, 0, 0)

                #cols = list(df.columns)
                counter = 1
                if isinstance(history_periods, str):
                    history_periods = int(history_periods)
                # make dataframes for request no. of periods
                start, end = self.shift_period_range(period, start, end)
                while counter < history_periods and start >= self.initial_date:
                    # load data
                    df_temp = self.load_df(start, end, table=self.table, cols=[], timestamp_col=timestamp_col)
                    df_temp = self.filter_df(df_temp)
                    df_temp[timestamp_col] = pd.to_datetime(df_temp[timestamp_col])
                    if df_temp is not None:
                        if len(df_temp) > 1:
                            string = '{} {}(s) prev'.format(counter, period)
                            # label period
                            df_temp = df_temp.assign(period=string)
                            # relabel days to get matching day of week,doy, dom, for different periods
                            df_temp = self.label_dates_pop(df_temp, period, timestamp_col)
                            df_temp = df_temp[cols]
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
            #df[timestamp_col] = pd.to_datetime(df[timestamp_col])
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
                    #logger.warning('LINE 218:%s', df.head(5))
                    df['dayset'] = df[timestamp_col].dt.dayofyear
                elif period == 'quarter':
                    df['dayset'] = df[timestamp_col].apply(lambda x: label_qtr_pop(x))

                return df
            except Exception:
                logger.error('label data ', exc_info=True)


        def get_groupby_pop_df(self,df,variable,groupby_cols):
            try:
                if variable in ['project']:
                    df = df.groupby(groupby_cols).agg({variable:'count'})
                    df = df.reset_index()
                    logger.warning('LINE 286 df:%s',df)
                elif variable in ['milestone']:
                    df = df.groupby(groupby_cols).agg({variable: 'count'})
                    df = df.reset_index()
                    logger.warning('LINE 291 df:%s', df)
                elif variable in ['task']:
                    df = df.groupby(groupby_cols).agg({variable: 'count'})
                    df = df.reset_index()
                elif variable in ['remuneration']:
                    df = df.groupby(groupby_cols).agg({variable: 'sum'})
                    df = df.reset_index()
                else:
                    #logger.warning('LINE 259:df:%s',df.head())
                    df = df.groupby(groupby_cols).agg({variable: 'mean'})
                    df = df.reset_index()

                # clean up
                if self.groupby_var in df.columns and self.variable != self.groupby_var:
                    df = df.drop([self.groupby_var],axis=1)

                return df
            except Exception:
                logger.error('get groupby card data', exc_info=True)

        def get_groupby_card_data(self,df,variable):
            try:
                if variable in ['project']:
                    data = len(df[variable].unique())
                    data = "{} {}s".format(data, variable)
                elif variable in ['milestone']:
                    df = df.groupby(['project']).agg({variable:'nunique'})
                    data = df[variable].sum()
                    data = "{} {}s".format(data, variable)
                elif variable in ['task']:
                    df = df.groupby(['project','milestone']).agg({variable:'count'})
                    data = df[variable].sum()
                    data = "{} {}s".format(data, variable)
                elif variable in ['project_duration'] or 'delay' in variable:
                    df = df.groupby([self.groupby_var]).agg({variable: 'mean'})
                    df = df.reset_index()
                    data = "{} days".format(round(df[variable].sum(), 2))
                elif variable in ['milestone_duration']:
                    df = df.groupby([self.groupby_var,'project']).agg({variable: 'mean'})
                    df = df.reset_index()
                    data = "{} days".format(round(df[variable].sum(), 2))
                elif variable in ['task_duration','task_start_delay', 'task_start_end']:
                    df = df.groupby([self.groupby_var,'project','milestone']).agg({variable: 'mean'})
                    df = df.reset_index()
                    data = "{} hours".format(round(df[variable].sum(), 2))
                elif variable in ['remuneration']:
                    data = df[variable].sum()
                    data = "${:,.2f}".format(data)

                return data
            except Exception:
                logger.error('get groupby card data', exc_info=True)



        # -------------------- GRAPHS -------------------------------------------
        def graph_periods_to_date(self, df2, timestamp_filter_col,variable):
            df1 = df2.copy()
            self.section_header_updater(section='cards',label=variable)
            try:
                df1 = self.filter_df(df1)

                for idx, period in enumerate(['week', 'month', 'quarter', 'year']):
                    df = self.period_to_date(df1, timestamp=dashboard_config['dates']['last_date'],
                                             timestamp_filter_col=timestamp_filter_col, period=period)

                    df = df.drop_duplicates(keep='first')

                    # groupby to eliminate repetition
                    data = self.get_groupby_card_data(df,variable)

                    del df
                    gc.collect()
                    title = "{} to date".format(period)

                    p = self.card(title=title, data=data, card_design=random.choice(list(self.KPI_card_css.keys())))
                    self.period_to_date_cards[period].text = p.text
                    logger.warning('%s to date completed', period)

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

                df = self.df_pop.copy()
                df = self.filter_df(df)
                #logger.warning('LINE 363 -df:%s',df.head())

                cols = [self.variable, self.timestamp_col]
                if self.variable != 'project':
                    cols.append('project')

                if abs(start_date - end_date).days > 7:
                    if 'week' in periods:
                        periods.remove('week')
                if abs(start_date - end_date).days > 31:
                    if 'month' in periods:
                        periods.remove('month')
                if abs(start_date - end_date).days > 90:
                    if 'quarter' in periods:
                        periods.remove('quarter')
                for idx, period in enumerate(periods):
                    df_period = self.period_over_period(df, start_date=start_date, end_date=end_date,
                                                        period=period, history_periods=self.pop_history_periods,
                                                        timestamp_col=self.timestamp_col)

                    groupby_cols = ['dayset', 'period']
                    df_period = self.get_groupby_pop_df(df_period,variable=self.variable,groupby_cols=groupby_cols)

                    prestack_cols = list(df_period.columns)
                    df_period = self.split_period_into_columns(df_period, col_to_split='period',
                                                               value_to_copy=self.variable)

                    # short term fix: filter out the unnecessary first day added by a corrupt quarter functionality
                    if period == 'quarter':
                        min_day = df_period['dayset'].min()
                        df_period = df_period[df_period['dayset'] > min_day]

                    poststack_cols = list(df_period.columns)

                    title = "{} over {}".format(period, period)
                    plotcols = list(np.setdiff1d(poststack_cols, prestack_cols))
                    if self.variable in ['task_start_delay','task_end_delay','task_duration']:
                        ylabel = 'hours'
                    elif self.variable in [
                        'project_duration','milestone_duration',
                        'project_start_delay','project_end_delay',
                        'milestone_start_delay','milestone_end_delay']:
                        ylabel = 'days'
                    elif self.variable in ['project','task','milestone']:
                        ylabel = '#'
                    elif self.variable == 'remuneration':
                        ylabel = '$'

                    if idx == 0:
                        p = df_period.hvplot.bar('dayset', plotcols, rot=45, title=title,
                                                 stacked=False, width=1200, height=400, value_label=ylabel)
                    else:
                        p += df_period.hvplot.bar('dayset', plotcols, rot=45, title=title,
                                                  stacked=False, width=1200, height=400, value_label=ylabel)
                return p

            except Exception:
                logger.error('period over period to date', exc_info=True)

    def update(attrname, old, new):
        thistab.notification_updater("Calculations underway. Please be patient")
        thistab.pm_gender = pm_gender_select.value
        thistab.m_gender = m_gender_select.value
        thistab.t_gender = t_gender_select.value

        thistab.type = type_select.value
        thistab.variable = variable_select.value
        if 'project' in thistab.variable:
            thistab.groupby_var = 'project'
        elif 'milestone' in thistab.variable:
            thistab.groupby_var = 'milestone'
        elif 'task' in thistab.variable:
            thistab.groupby_var = 'task'

        thistab.status = status_select.value
        thistab.graph_periods_to_date(thistab.df, thistab.timestamp_col, variable=thistab.variable)
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        thistab.notification_updater("ready")

    def update_pop_dates():
        thistab.notification_updater("Calculations underway. Please be patient")
        thistab.pop_history_periods = pop_number_select.value
        thistab.pop_start_date = datepicker_pop_start.value  # trigger period over period
        thistab.pop_end_date = datepicker_pop_end.value
        thistab.df_pop = thistab.pym.load_df(start_date=thistab.pop_start_date,
                                         end_date=thistab.pop_end_date,
                                         cols=[], table=thistab.table, timestamp_col='startdate_actual')
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        thistab.notification_updater("ready")

    def update_history_periods(attrname, old, new):
        thistab.notification_updater("Calculations underway. Please be patient")
        thistab.pop_history_periods = pop_number_select.value
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        thistab.notification_updater("ready")

    try:
        cols = []
        thistab = Thistab(table='project_composite', cols=cols)
        # -------------------------------------  SETUP   ----------------------------
        # format dates
        first_date_range = thistab.initial_date
        last_date_range = datetime.now().date()
        last_date = dashboard_config['dates']['last_date']
        first_date = datetime(last_date.year, 1, 1, 0, 0, 0)

        thistab.df = thistab.pym.load_df(start_date=first_date,end_date=last_date,table=thistab.table,cols=[],
                                     timestamp_col=thistab.timestamp_col)
        thistab.graph_periods_to_date(thistab.df, timestamp_filter_col=thistab.timestamp_col, variable=thistab.variable)
        thistab.pop_end_date = last_date
        thistab.pop_start_date = last_date - timedelta(days=5)
        thistab.df_pop = thistab.pym.load_df(start_date=thistab.pop_start_date,
                                             end_date=thistab.pop_end_date,
                                             cols=[], table=thistab.table,
                                             timestamp_col=thistab.timestamp_col)

        # MANAGE STREAM
        # date comes out stream in milliseconds
        # --------------------------------CREATE WIDGETS ---------------------------------


        stream_launch = streams.Stream.define('Launch', launch=-1)()

        datepicker_pop_start = DatePicker(title="Period start", min_date=first_date_range,
                                        max_date=last_date_range, value=thistab.pop_start_date)

        datepicker_pop_end = DatePicker(title="Period end", min_date=first_date_range,
                                        max_date=last_date_range, value=thistab.pop_end_date)

        pop_number_select = Select(title='Select # of comparative periods',
                                   value=str(thistab.pop_history_periods),
                                   options=thistab.menus['history_periods'])
        pop_dates_button = Button(label="Select dates, then click me!", width=15, button_type="success")

        type_select = Select(title='Select project type', value=thistab.type,
                                 options=thistab.menus['type'])

        status_select = Select(title='Select project status', value=thistab.status,
                                     options=thistab.menus['status'])

        pm_gender_select = Select(title="Select project owner's gender", value=thistab.pm_gender,
                                       options=thistab.menus['gender'])

        m_gender_select = Select(title="Select milestone owner's gender", value=thistab.m_gender,
                                      options=thistab.menus['gender'])

        t_gender_select = Select(title="Select task owner's gender", value=thistab.t_gender,
                                      options=thistab.menus['gender'])

        variable_select = Select(title='Select variable of interest', value=thistab.variable,
                               options=thistab.menus['variables'])

        # ---------------------------------  GRAPHS ---------------------------

        hv_pop_week = hv.DynamicMap(thistab.pop_week, streams=[stream_launch])
        pop_week = renderer.get_plot(hv_pop_week)

        hv_pop_month = hv.DynamicMap(thistab.pop_month, streams=[stream_launch])
        pop_month = renderer.get_plot(hv_pop_month)

        hv_pop_quarter = hv.DynamicMap(thistab.pop_quarter, streams=[stream_launch])
        pop_quarter = renderer.get_plot(hv_pop_quarter)

        hv_pop_year = hv.DynamicMap(thistab.pop_year, streams=[stream_launch])
        pop_year = renderer.get_plot(hv_pop_year)

        # -------------------------------- CALLBACKS ------------------------

        type_select.on_change('value', update)
        pop_dates_button.on_click(update_pop_dates)  # lags array
        status_select.on_change('value', update)
        pm_gender_select.on_change('value', update)
        m_gender_select.on_change('value', update)
        t_gender_select.on_change('value', update)
        variable_select.on_change('value', update)
        pop_number_select.on_change('value',update_history_periods)


        # -----------------------------------LAYOUT ----------------------------
        # put the controls in a single element

        controls_pop_left = WidgetBox(datepicker_pop_start)
        controls_pop_centre = WidgetBox(datepicker_pop_end)
        controls_pop_right = WidgetBox(pop_dates_button)

        grid = gridplot([
            [thistab.notification_div['top']],
            [variable_select,type_select,status_select],
            [pm_gender_select, m_gender_select, t_gender_select],
            [thistab.section_headers['cards']],
            [thistab.period_to_date_cards['year'], thistab.period_to_date_cards['quarter'],
             thistab.period_to_date_cards['month'], thistab.period_to_date_cards['week']],
            [thistab.section_headers['pop'], controls_pop_right],
            [controls_pop_left, controls_pop_centre, pop_number_select],
            [pop_week.state],
            [pop_month.state],
            [pop_quarter.state],
            [pop_year.state],
            [thistab.notification_div['bottom']]
        ])

        # Make a tab with the layout
        tab = Panel(child=grid, title=panel_title)
        return tab

    except Exception:
        logger.error('rendering err:', exc_info=True)
        return tab_error_flag(panel_title)
