import random

from holoviews import streams

from config.BCC import cols_to_load
from scripts.databases.pythonMongo import PythonMongo
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
    DatePicker, Select, Button

from datetime import datetime, date, timedelta

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
def kpi_bcc_rentals_visitor_tab(panel_title):
    class Thistab(KPI):
        def __init__(self, table, cols=[]):
            KPI.__init__(self, table,name='rentals',cols=cols)
            self.table = table
            self.df = None
            self.pym = PythonMongo('aion')

            self.checkboxgroup = {
                'category': [],
                'item' : [],
                'area':[],
                'visit_duration':[]
            }

            self.multiline_vars = {
                'y' : 'visit_duration'
            }

            self.groupby_dict = {
                'item':'count',
                'area':'count',
                'category':'count',
                'status':'count',
                'gender':'count',
                'visit_duration':'sum'
            }
            # setup selects

            self.select_values = {}
            self.select_menus = {}
            for item in ['area', 'item', 'category', 'status', 'gender']:
                self.select_values[item] = 'all'
                self.select_menus[item] = ['all']

            self.select = {}
            for item in ['area', 'item', 'category', 'status', 'gender']:
                self.select[item] = Select(title='Select ' + item, value='all',
                                           options=self.select_menus[item])
            self.timestamp_col = 'visit_start'

            self.variable = 'item'
            
            self.multiline_resample_period = 'M'

            self.ptd_startdate = datetime(datetime.today().year, 1, 1, 0, 0, 0)

            # cards

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
                'dow': self.section_header_div(text='Compare days of the week:'.format(self.section_divider),
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
e7                </li>
                 <li>
                </li>
                 <li>
                </li>
            </ul>
            </div>

            """.format(div_style, self.header_style)
            div = Div(text=txt, width=width, height=height)
            return div

        # ------------------- LOAD AND SETUP DATA -----------------------
        def filter_df(self, df):
            try:
                for item in self.select_values.keys():
                    if self.select_values[item] != 'all':
                        df = df[df[item] == self.select_values[item]]
                return df

            except Exception:
                logger.error('filters', exc_info=True)

        def set_select_menus(self,df):
            try:
                for item in self.select.keys():
                    if item in df.columns and len(df) > 0:
                        logger.warning('LINE 151: item: %s', item)
                        lst = list(set(df[item].values))
                        lst.append('all')
                        sorted(lst)
                        logger.warning('LINE 157: LIST: %s',lst)

                        self.select[item].options = lst

            except Exception:
                logger.error('set filters menus', exc_info=True)


        def load_df_pym(self, req_startdate, req_enddate, cols, timestamp_col):
            try:
                # get min and max of loaded df
                if self.df is not None:
                    loaded_min = self.df[timestamp_col].min()
                    loaded_max = self.df[timestamp_col].max()

                    if loaded_min <= req_startdate and loaded_max >= req_enddate:
                        df = self.df[(self.df[timestamp_col] >= req_startdate) &
                                     (self.df[timestamp_col] <= req_enddate)]
                        df = self.filter_df(df)
                    else:
                        df = self.pym.load_df(req_startdate, req_enddate, table=self.table,
                                         cols=cols, timestamp_col=timestamp_col)

                else:
                    df = self.pym.load_df(req_startdate, req_enddate, table=self.table,
                                          cols=cols, timestamp_col=timestamp_col)

                df = self.filter_df(df)
                #logger.warning('LINE 185: item: %s', df.head())
                self.set_select_menus(df)

                return df

            except Exception:
                logger.error('load_df', exc_info=True)

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


        def graph_periods_to_date(self, df1, timestamp_filter_col, variable):
            try:
                dct = {}
                for idx, period in enumerate(['week', 'month', 'quarter', 'year']):
                    df = self.period_to_date(df1, timestamp=dashboard_config['dates']['last_date'],
                                             timestamp_filter_col=timestamp_filter_col, period=period)

                    # get unique instances
                    df = df[[variable]]
                    df = df.drop_duplicates(keep='first')
                    #logger.warning('post duplicates dropped:%s', df.head(10))
                    data  = 0
                    if self.groupby_dict[variable] == 'sum':
                        data = int(df[variable].sum())
                    elif self.groupby_dict[variable] == 'mean':
                        data = "{}%".format(round(df[variable].mean(),3))
                    else:
                        data = int(df[variable].count())
                    del df
                    gc.collect()
                    dct[period] = data

                self.update_cards(dct)


            except Exception:
                logger.error('graph periods to date', exc_info=True)

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
                #logger.warning('LINE 244:%s', df_current.head(15))
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

                    df_temp = self.load_df_pym(start, end, cols, timestamp_col)
                    df_temp[timestamp_col] = pd.to_datetime(df_temp[timestamp_col])
                    if df_temp is not None:
                        if len(df_temp) > 1:
                            string = '{} {}(s) prev'.format(counter, period)
                            # label period
                            df_temp[period] = string
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

        def graph_period_over_period(self,period):
            try:
                periods = [period]
                start_date = self.pop_start_date
                end_date = self.pop_end_date
                if isinstance(start_date,date):
                    start_date = datetime.combine(start_date,datetime.min.time())
                if isinstance(end_date,date):
                    end_date = datetime.combine(end_date,datetime.min.time())
                #cols = [self.variable, self.timestamp_col, 'day']
                cols = [self.variable, self.timestamp_col]

                df = self.load_df_pym(start_date,end_date,cols=cols,
                                  timestamp_col=self.timestamp_col)


                for idx,period in enumerate(periods):
                    df_period = self.period_over_period(df, start_date=start_date, end_date=end_date,
                                                        period=period, history_periods=self.pop_history_periods,
                                                        timestamp_col=self.timestamp_col)
                    logger.warning('LINE 274 start:end=%s:%s,%s,%s len(Df),df.head',start_date,end_date,len(df),df.head())
                    groupby_cols = ['dayset','period']
                    if len(df_period) > 0:
                        df_period = df_period.groupby(groupby_cols).agg({self.variable:'count'})
                        df_period = df_period.reset_index()
                    else:
                        df_period = df_period.rename(index=str,columns={'day':'dayset'})

                    prestack_cols = list(df_period.columns)
                    logger.warning('Line 179:%s', df_period.head(10))
                    df_period = self.split_period_into_columns(df_period,col_to_split='period',
                                                               value_to_copy=self.variable)
                    logger.warning('line 180 df_period columns:%s',df_period.head(50))
                    poststack_cols = list(df_period.columns)
                    title = "{} over {}".format(period,period)


                    plotcols = list(np.setdiff1d(poststack_cols, prestack_cols))
                    df_period,plotcols = self.pop_include_zeros(df_period=df_period,plotcols=plotcols,period=period)

                    if 'dayset' not in df_period.columns:
                        leng = len(df_period)
                        if leng > 0:
                            df_period['dayset'] = 0
                        else:
                            df_period['dayset'] = ''

                    if idx == 0:
                        p = df_period.hvplot.bar('dayset',plotcols,rot=45,title=title,
                                                 stacked=False)
                    else:
                        p += df_period.hvplot.bar('dayset',plotcols,rot=45,title=title,
                                                  stacked=False)
                return p

            except Exception:
                logger.error('period over period to date', exc_info=True)

        def pop_week(self, launch=-1):
            try:
                return self.graph_period_over_period('week')
            except Exception:
                logger.error('pop week', exc_info=True)

        def pop_month(self, launch=-1):
            try:
                return self.graph_period_over_period('month')
            except Exception:
                logger.error('pop month', exc_info=True)

        def pop_quarter(self, launch=-1):
            try:
                return self.graph_period_over_period('quarter')
            except Exception:
                logger.error('pop quarter', exc_info=True)

        def pop_year(self, launch=-1):
            try:
                return self.graph_period_over_period('year')
            except Exception:
                logger.error('pop year', exc_info=True)

        def multiline_dow(self, launch=1):
            try:
                df = self.df.copy()
                dct = {
                    'Y':'year',
                    'M':'month',
                    'W':'week',
                    'Q':'Qtr'
                }
                resample_period =  dct[self.multiline_resample_period]
                yvar = self.multiline_vars['y']
                xvar = 'day_of_week'
                df[resample_period] = df[self.timestamp_col].dt.to_period(self.multiline_resample_period)
                df[xvar] = df[self.timestamp_col].dt.day_name()
                df = df.groupby([xvar,resample_period]).agg({yvar: 'mean'})
                df = df.reset_index()

                p = df.hvplot.line(x=self.timestamp_col, y=yvar,groupby=resample_period, width=1200, height=500)
                return p
            except Exception:
                logger.error('multiline plot', exc_info=True)

    def update(attrname, old, new):
        thistab.notification_updater("Calculations underway. Please be patient")
        for item in ['area','category','gender','item']:
            thistab.select_values[item] = thistab.select[item].value
            
        thistab.graph_periods_to_date(thistab.df,thistab.timestamp_col)
        thistab.section_header_updater('cards')
        thistab.section_header_updater('pop')
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        thistab.notification_updater("ready")

    def update_variable(attrname, old, new):
        thistab.notification_updater("Calculations underway. Please be patient")
        thistab.variable = variable_select.value
        thistab.graph_periods_to_date(thistab.df,'block_timestamp',thistab.variable)
        thistab.section_header_updater('cards',label='')
        thistab.section_header_updater('pop',label='')
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

    def update_multiline(attrname, old, new):
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.multiline_vars['y'] = multiline_y_select.value
        thistab.multiline_resample_period = multiline_resample_period_select.value
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    try:
        table = 'bcc_composite'
        cols = cols_to_load['guest'] + cols_to_load['rental']
        thistab = Thistab(table, cols)
        # -------------------------------------  SETUP   ----------------------------
        # format dates
        first_date_range = thistab.initial_date
        last_date_range = datetime.now().date()

        last_date = dashboard_config['dates']['last_date']
        first_date = datetime(2014,1,1,0,0,0)

        thistab.df = thistab.load_df_pym(first_date, last_date,cols,thistab.timestamp_col)
        thistab.graph_periods_to_date(thistab.df,timestamp_filter_col=thistab.timestamp_col,
                                      variable=thistab.variable)
        thistab.section_header_updater('cards')
        thistab.section_header_updater('pop')

        '''
        df_temp = thistab.df[(thistab.df['visit_start'].dt.year == 2019) & (thistab.df['visit_start'].dt.month == 8)]

        logger.warning('LINE 416: df_temp:%s,%s',len(df_temp),df_temp.head(30))
        '''

        # MANAGE STREAM
        # date comes out stream in milliseconds
        # --------------------------------CREATE WIDGETS ---------------------------------
        datepicker_start = DatePicker(title="Start", min_date=first_date_range,
                                      max_date=last_date_range, value=first_date)
        datepicker_end = DatePicker(title="End", min_date=first_date_range,
                                    max_date=last_date_range, value=last_date)

        thistab.pop_end_date = datetime.now().date()
        daynum = thistab.pop_end_date.day
        if daynum < 3:
            thistab.pop_end_date = datetime.now().date() - timedelta(days=daynum)
            thistab.pop_start_date = thistab.pop_end_date - timedelta(days=7)
        else:
            thistab.pop_start_date = thistab.first_date_in_period(thistab.pop_end_date, 'week')

        stream_launch = streams.Stream.define('Launch',launch=-1)()

        datepicker_period_start = DatePicker(title="Period start", min_date=first_date_range,
                                             max_date=last_date_range, value=thistab.pop_start_date)
        datepicker_period_end = DatePicker(title="Period end", min_date=first_date_range,
                                           max_date=last_date_range, value=thistab.pop_end_date)

        history_periods_select = Select(title='Select # of comparative periods',
                                        value='2',
                                        options=thistab.menus['history_periods'])

        datepicker_pop_start = DatePicker(title="Period start", min_date=first_date_range,
                                        max_date=last_date_range, value=thistab.pop_start_date)

        datepicker_pop_end = DatePicker(title="Period end", min_date=first_date_range,
                                        max_date=last_date_range, value=thistab.pop_end_date)

        pop_number_select = Select(title='Select # of comparative periods',
                                   value=str(5),
                                   options=thistab.menus['history_periods'])
        pop_button = Button(label="Select dates/periods, then click me!", width=15, button_type="success")

        variable_select = Select(title='Select variable', value=thistab.variable,
                                 options=thistab.menus['bcc']['rental'])

        multiline_y_select = Select(title='Select comparative DV(y)',
                                    value=thistab.multiline_vars['y'],
                                    options=['price', 'amount', 'visit_duration'])

        multiline_resample_period_select = Select(title='Select comparative DV(y)',
                                    value=thistab.multiline_resample_period,
                                    options=['W','M','Q','Y'])

        # ---------------------------------  GRAPHS ---------------------------
        hv_pop_week = hv.DynamicMap(thistab.pop_week, streams=[stream_launch])
        pop_week = renderer.get_plot(hv_pop_week)

        hv_pop_month = hv.DynamicMap(thistab.pop_month, streams=[stream_launch])
        pop_month = renderer.get_plot(hv_pop_month)

        hv_pop_quarter = hv.DynamicMap(thistab.pop_quarter, streams=[stream_launch])
        pop_quarter = renderer.get_plot(hv_pop_quarter)

        hv_multiline_dow = hv.DynamicMap(thistab.multiline_dow, streams=[stream_launch])
        multiline_dow = renderer.get_plot(hv_multiline_dow)

        # -------------------------------- CALLBACKS ------------------------
        #datepicker_start.on_change('value', update)
        #datepicker_end.on_change('value', update)
        for item in ['area','category','gender','item']:
            thistab.select[item].on_change('value',update)
        
        history_periods_select.on_change('value',update_period_over_period)
        datepicker_period_start.on_change('value',update_period_over_period)
        datepicker_period_end.on_change('value',update_period_over_period)
        pop_number_select.on_change('value',update_history_periods)
        variable_select.on_change('value', update_variable)
        multiline_y_select.on_change('value', update_multiline)
        multiline_resample_period_select.on_change('value', update_multiline)

        # -----------------------------------LAYOUT ----------------------------
        # put the controls in a single element
        controls = WidgetBox(datepicker_start,datepicker_end, 
                             thistab.select['area'],
                             thistab.select['category'],
                             thistab.select['item'],
                             thistab.select['gender'],
                             thistab.select['status'],
                             )
        controls_pop = WidgetBox(datepicker_pop_start,
                                 datepicker_pop_end,
                                 history_periods_select,
                                 pop_button)

        controls_multiline = WidgetBox(multiline_y_select,
                                       multiline_resample_period_select)

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
            [thistab.section_headers['dow']],
            [Spacer(width=20, height=30)],
            [multiline_dow.state, controls_multiline],
            [thistab.notification_div['bottom']]
        ])

        # Make a tab with the layout
        tab = Panel(child=grid, title=panel_title)
        return tab

    except Exception:
        logger.error('rendering err:', exc_info=True)
        return tab_error_flag(panel_title)


