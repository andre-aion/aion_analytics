from datetime import datetime, timedelta, date

import pydot
from bokeh.layouts import gridplot
from bokeh.models import Panel, Div, DatePicker, WidgetBox, Button, Select, Spacer
from sklearn import metrics
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix, classification_report
from sklearn.model_selection import train_test_split
from sklearn.tree import export_graphviz
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline

from scripts.databases.pythonClickhouse import PythonClickhouse
from scripts.utils.interfaces.mytab_interface import Mytab
from scripts.utils.mylogger import mylogger
from scripts.streaming.streamingDataframe import StreamingDataframe as SD
from config.dashboard import config as dashboard_config

from tornado.gen import coroutine

from operator import itemgetter
import pandas as pd
import dask as dd
import holoviews as hv
from holoviews import streams

from scripts.utils.myutils import tab_error_flag
from config.hyp_variables import groupby_dict
from fbprophet import Prophet
logger = mylogger(__file__)

hv.extension('bokeh', logo=False)
renderer = hv.renderer('bokeh')

table = 'accounts_predictive'
hyp_variables= list(groupby_dict[table].keys())
menus = {
    'account_type' : ['all','contract','miner','native_user','token_user'],
    'update_type': ['all','contract_deployment','internal_transfer','mined_block','token_transfer','transaction'],
    'status' : ['all','active','churned','joined']
}

@coroutine
def accounts_tsa_tab(panel_title):
    class Thistab(Mytab):
        def __init__(self, table, cols, dedup_cols):
            Mytab.__init__(self, table, cols, dedup_cols)
            self.table = table
            self.cols = cols
            self.DATEFORMAT = "%Y-%m-%d %H:%M:%S"
            self.df = None
            self.df1 = {}  # to contain churned and retained splits
            self.df_predict = None
            self.day_diff = 1  # for normalizing for classification periods of different lengths
            self.df_grouped = ''

            self.rf = {}  # random forest
            self.cl = PythonClickhouse('aion')

            self.forecast_days = 30
            self.interest_var = 'address'
            self.trigger = -1
            self.status = 'all'
            self.update_type = 'all'
            self.status = 'all'
            self.account_type = 'all'
            self.interest_var = 'amount'

            self.pl = {}  # for rf pipeline
            self.div_style = """ style='width:300px; margin-left:25px;
            border:1px solid #ddd;border-radius:3px;background:#efefef50;' 
            """
            self.header_style = """ style='color:blue;text-align:center;' """

            # list of tier specific addresses for prediction
            self.address_list = []
            self.address_select = Select(
                title='Filter by address',
                value='all',
                options=[])
            self.address = 'all'
            self.load_data_flag = False
            self.day_diff = 1
            self.groupby_dict = {}

            self.div_style = """ style='width:300px; margin-left:25px;
                        border:1px solid #ddd;border-radius:3px;background:#efefef50;' 
                        """
            self.max_loaded_date = None
            self.min_loaded_date = None

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

                'forecast': self.section_header_div(text='Forecasts:{}'.format(self.section_divider),
                                                             width=600, html_header='h2', margin_top=5,
                                                             margin_bottom=-155),
            }


            # ----------------------  DIVS ----------------------------

        def section_header_div(self, text, html_header='h2', width=600, margin_top=150, margin_bottom=-150):
            text = """<div style="margin-top:{}px;margin-bottom:-{}px;"><{} style="color:#4221cc;">{}</{}></div>""" \
                .format(margin_top, margin_bottom, html_header, text, html_header)
            return Div(text=text, width=width, height=15)


            # ####################################################
            #              UTILITY DIVS

        def results_div(self, text, width=600, height=300):
            div = Div(text=text, width=width, height=height)
            return div

        def title_div(self, text, width=700):
            text = '<h2 style="color:#4221cc;">{}</h2>'.format(text)
            return Div(text=text, width=width, height=15)

        def reset_checkboxes(self):
            try:
                self.address_selected = ""
                self.address_select.value = "all"
            except Exception:
                logger.error('reset checkboxes', exc_info=True)

        ###################################################
        #               I/O
        def load_df(self, start_date, end_date):
            try:
                logger.warning("data load begun")
                if isinstance(start_date, str):
                    start_date = datetime.strptime(start_date, self.DATEFORMAT)
                if isinstance(end_date, str):
                    end_date = datetime.strptime(end_date, self.DATEFORMAT)

                if self.df is not None:
                    self.max_loaded_date = self.df.block_timestamp.max().compute()
                    self.min_loaded_date = self.df.block_timestamp.min().compute()
                    if start_date >= self.min_loaded_date and end_date <= self.max_loaded_date:
                        logger.warning("data already loaded - %s", self.df.tail(10))
                        pass
                    else:
                        self.df_load(start_date, end_date,cols=self.cols)
                        self.df = self.df.fillna(0)
                        #self.make_delta()
                        #self.df = self.df.set_index('block_timestamp')
                        logger.warning("data loaded - %s",self.df.tail(10))
                else:
                    self.df_load(start_date, end_date, cols=self.cols)
                    self.df = self.df.fillna(0)
                    # self.make_delta()
                    # self.df = self.df.set_index('block_timestamp')
                    logger.warning("data loaded - %s", self.df.tail(10))
                    self.df = self.filter(self.df)


            except Exception:
                logger.error('load_df', exc_info=True)

        ###################################################
        #               MUNGE DATA
        def make_delta(self):
            try:
                if self.df is not None:
                    if len(self.df) > 0:
                        df = self.df.compute()
                        for col in self.targets:
                            col_new = col + '_diff'
                            df[col_new] = df[col].pct_change()
                            df[col_new] = df[col_new].fillna(0)
                            logger.warning('diff col added : %s', col_new)
                        self.df = self.df.fillna(self.df.mean())
                        self.df = dd.dataframe.from_pandas(df, npartitions=15)
                        # logger.warning('POST DELTA:%s',self.df1.tail(20))

            except Exception:
                logger.error('make delta', exc_info=True)



        ##################################################
        #               EXPLICATORY GRAPHS
        # PLOTS
        def box_plot(self, variable):
            try:
                # logger.warning("difficulty:%s", self.df.tail(30))
                # get max value of variable and multiply it by 1.1
                minv = 0
                maxv = 0
                df = self.df
                if df is not None:
                    if len(df) > 0:
                        minv, maxv = dd.compute(df[variable].min(),
                                                df[variable].max())
                else:
                    df = SD('filter', [variable, 'status'], []).get_df()

                return df.hvplot.box(variable, by='status',
                                                  ylim=(.9 * minv, 1.1 * maxv))
            except Exception:
                logger.error("box plot:", exc_info=True)



        ###################################################
        #               MODELS

        def filter(self,df):
            try:
                df = df.assign(freq=df.address)
                if self.status != 'all':
                    df = df[df.status == self.status]
                if self.account_type != 'all':
                    df = df[df.acccount_type == self.account_type]
                if self.update_type != 'all':
                    df = df[df.update_type == self.update_type]
                if self.address != 'all':
                    df = df[df.address == self.address]

                return df
            except Exception:
                logger.error("filter:", exc_info=True)



        def tsa_amount(self,launch):
            try:
                logger.warning('df columns:%s',list(self.df.columns))
                df = self.df.set_index('block_timestamp')
                df = df.resample('D').agg({'amount': 'mean'})
                df = df.reset_index()
                df = df.compute()
                label = 'amount_diff'
                df[label] = df[self.interest_var].diff()
                df = df.fillna(0)

                rename = {'block_timestamp':'ds','amount':'y'}
                df = df.rename(columns=rename)
                logger.warning('df:%s',df.head())
                df = df[['ds','y']]
                logger.warning('df:%s',df.tail())
                m = Prophet()
                m.fit(df)

                future = m.make_future_dataframe(periods=5)
                forecast = m.predict(future)
                print(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail())
                print(list(forecast.columns))
                for idx,col in enumerate(['yhat', 'yhat_lower', 'yhat_upper']):
                    if idx == 0:
                        p = forecast.hvplot.line(x='ds',y=col,width=550,height=250).relabel(col)
                    else:
                        p *= forecast.hvplot.scatter(x='ds'
                                                     ,y=col,width=550,height=250).relabel(col)

                for idx,col in enumerate(['trend','weekly']):
                    if idx == 0:
                        q = forecast.hvplot.line(x='ds',y=col,width=550).relabel(col)
                    else:
                        q *= forecast.hvplot.line(x='ds',y=col,width=550,height=250).relabel(col)

                return p + q
            except Exception:
                logger.error("box plot:", exc_info=True)

        def tsa_freq(self, launch):
            try:
                logger.warning('df columns:%s', list(self.df.columns))
                df = self.df.set_index('block_timestamp')
                df = df.resample('D').agg({'address': 'nunique'})
                df = df.reset_index()
                df = df.compute()
                label = 'freq_diff'
                df[label] = df['address'].diff()
                df = df.fillna(0)

                rename = {'block_timestamp': 'ds', 'address': 'y'}
                df = df.rename(columns=rename)
                logger.warning('df:%s', df.head())
                df = df[['ds', 'y']]
                logger.warning('df:%s', df.tail())
                m = Prophet()
                m.fit(df)

                future = m.make_future_dataframe(periods=5)
                forecast = m.predict(future)
                print(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail())
                print(list(forecast.columns))
                for idx, col in enumerate(['yhat', 'yhat_lower', 'yhat_upper']):
                    if idx == 0:
                        p = forecast.hvplot.line(x='ds', y=col, width=550, height=250).relabel(col)
                    else:
                        p *= forecast.hvplot.scatter(x='ds'
                                                     , y=col, width=550, height=250).relabel(col)

                for idx, col in enumerate(['trend', 'weekly']):
                    if idx == 0:
                        q = forecast.hvplot.line(x='ds', y=col, width=550, height=250).relabel(col)
                    else:
                        q *= forecast.hvplot.line(x='ds', y=col, width=550, height=250).relabel(col)

                return p + q
            except Exception:
                logger.error("box plot:", exc_info=True)

        ####################################################
        #               GRAPHS
    def update(attrname, old, new):
        thistab.notification_updater("Calculations underway. Please be patient")
        thistab.update_type = update_type_select.value
        thistab.status = status_select.value
        thistab.account_type = account_type_select.value
        thistab.forecast_days = int(select_forecast_days.value)
        thistab.address = thistab.address_select.value
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        stream_select_variable.event(variable=thistab.inspected_variable)
        thistab.notification_updater("ready")

    def update_load(attrname, old, new):
        thistab.notification_updater("Calculations underway. Please be patient")
        thistab.load_df(datepicker_start.value, datepicker_end.value)

        thistab.notification_updater("ready")

    try:
        # SETUP
        table = 'account_ext_warehouse'
        #cols = list(table_dict[table].keys())

        cols = ['address','block_timestamp','account_type','status','update_type','amount']
        thistab = Thistab(table, cols, [])


        # setup dates
        first_date_range = datetime.strptime("2018-04-25 00:00:00", "%Y-%m-%d %H:%M:%S")
        last_date_range = datetime.now().date()
        last_date = dashboard_config['dates']['last_date']
        first_date = last_date - timedelta(days=60)
        # STREAMS Setup
        # date comes out stream in milliseconds
        stream_launch = streams.Stream.define('Launch',launch=-1)()
        stream_select_variable = streams.Stream.define('Select_variable',
                                                       variable='amount')()

        # setup widgets
        datepicker_start = DatePicker(title="Start", min_date=first_date_range,
                                      max_date=last_date_range, value=first_date)
        datepicker_end = DatePicker(title="End", min_date=first_date_range,
                                    max_date=last_date_range, value=last_date)
        select_forecast_days = Select(title='Select # of days which you want forecasted',
                                      value=str(thistab.forecast_days),
                                      options=['10','20','30','40','50','60','70','80','90'])
        status_select = Select(title='Select account status',
                               value=thistab.status,
                               options=menus['status'])
        account_type_select = Select(title='Select account type',
                                     value=thistab.account_type,
                                     options=menus['account_type'])
        update_type_select = Select(title='Select transfer type',
                                    value=thistab.update_type,
                                    options=menus['update_type'])
        # search by address checkboxes

        reset_prediction_address_button = Button(label="reset address(es)", button_type="success")

        # ----------------------------------- LOAD DATA
        # load model-making data
        thistab.load_df(datepicker_start.value, datepicker_end.value)
        # load data for period to be predicted

        # tables
        hv_tsa_amount = hv.DynamicMap(thistab.tsa_amount,streams=[stream_launch])
        tsa_amount = renderer.get_plot(hv_tsa_amount)

        hv_tsa_freq = hv.DynamicMap(thistab.tsa_freq, streams=[stream_launch])
        tsa_freq = renderer.get_plot(hv_tsa_freq)

        # add callbacks
        datepicker_start.on_change('value', update_load)
        datepicker_end.on_change('value', update_load)
        thistab.address_select.on_change('value', update)
        reset_prediction_address_button.on_click(thistab.reset_checkboxes)
        select_forecast_days.on_change('value',update)
        update_type_select.on_change('value', update)
        account_type_select.on_change('value', update)
        status_select.on_change('value', update)

        # put the controls in a single element
        controls = WidgetBox(
            datepicker_start, datepicker_end,
            thistab.address_select,
            reset_prediction_address_button,
            select_forecast_days,
            update_type_select,
            account_type_select,
            status_select,
        )

        grid = gridplot([
            [thistab.notification_div['top']],
            [Spacer(width=20, height=70)],
            [thistab.section_headers['forecast']],
            [Spacer(width=20, height=30)],
            [tsa_amount.state,controls],
            [tsa_freq.state],
            [thistab.notification_div['bottom']]
        ])

        tab = Panel(child=grid, title=panel_title)
        return tab

    except Exception:
        logger.error('rendering err:', exc_info=True)
        return tab_error_flag(panel_title)
