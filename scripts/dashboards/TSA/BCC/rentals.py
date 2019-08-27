from datetime import datetime, timedelta

from bokeh.layouts import gridplot
from bokeh.models import Panel, Div, DatePicker, WidgetBox, Button, Select, TableColumn, ColumnDataSource, DataTable, \
    Spacer
from fbprophet import Prophet

from scripts.databases.pythonClickhouse import PythonClickhouse
from scripts.databases.pythonMongo import PythonMongo
from scripts.utils.interfaces.mytab_interface import Mytab
from scripts.utils.mylogger import mylogger
from config.dashboard import config as dashboard_config
from bokeh.models.widgets import TextInput

from tornado.gen import coroutine

import pandas as pd
import numpy as np
import holoviews as hv
from holoviews import streams

from scripts.utils.myutils import tab_error_flag
from config.BCC import cols_to_load

logger = mylogger(__file__)

hv.extension('bokeh', logo=False)
renderer = hv.renderer('bokeh')


@coroutine
def forecasting_bcc_rentals_visitor_tab(panel_title):

    class Thistab(Mytab):
        def __init__(self, table, cols, dedup_cols=[]):
            Mytab.__init__(self, table, cols, dedup_cols)
            self.table = table
            self.cols = cols
            self.DATEFORMAT = "%Y-%m-%d %H:%M:%S"
            self.df = None
            self.df1 = None
            self.df_predict = None
            self.day_diff = 1  # for normalizing for classification periods of different lengths
            self.df_grouped = ''

            self.cl = PythonClickhouse('aion')

            self.trigger = 0
            self.groupby_dict = {
                'category': 'nunique',
                'item': 'nunique',
                'area' : 'nunique',
                'visit_duration': 'mean',
                'age': 'mean',
                'gender_coded':'mean',
                'status_coded':'mean',
                'rental_employee_gender_coded':'mean',
                'rental_employee_age':'mean',
                'rental_tab':'sum'

            }

            self.feature_list = ['age','rental_employee_age','rental_tab']
            self.tsa_variable = 'rental_tab'
            self.forecast_days = 40
            self.lag_variable = 'visit_duration'
            self.lag_days = "1,2,3"
            self.lag = 0
            self.lag_menu = [str(x) for x in range(0, 100)]

            self.strong_thresh = .65
            self.mod_thresh = 0.4
            self.weak_thresh = 0.25
            self.corr_df = None
            self.div_style = """ 
                style='width:350px; margin-left:25px;
                border:1px solid #ddd;border-radius:3px;background:#efefef50;' 
            """

            self.header_style = """ style='color:blue;text-align:center;' """

            self.variables = sorted(list(self.groupby_dict.keys()))
            self.variable = 'rental_tab'

            self.relationships_to_check = ['weak', 'moderate', 'strong']


            self.pym = PythonMongo('aion')
            self.menus = {
                'item':['all'],
                'category':['all'],
                'status':['all','guest','member'],
                'gender': ['all', 'male', 'female'],
                'variables': list(self.groupby_dict.keys()),
                'history_periods': ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'],
                'area':['all','bar','rentals'],
                'tsa':['rental_tab','visit_duration']
            }
            self.select = {}
            self.select['area'] = Select(title='Select BCC area', value='all',
                                        options=self.menus['area'])

            self.select['item'] = Select(title='Select item', value='all',
                                        options=self.menus['item'])

            self.select['status'] = Select(title='Select visitor status', value='all',
                                            options=self.menus['status'])

            self.select['gender'] = Select(title="Select visitor gender", value='all',
                                            options=self.menus['gender'])

            self.select['category'] = Select(title="Select category", value='all',
                                            options=self.menus['category'])

            self.select['rental_employee_gender'] = Select(title="Select category", value='all',
                                             options=self.menus['category'])



            self.select_values = {}
            for item in self.select.keys():
                self.select_values[item] = 'all'

            self.multiline_vars = {
                'x': 'gender',
                'y': 'rental_tab'
            }
            self.timestamp_col = 'visit_start'
            # ------- DIVS setup begin
            self.page_width = 1250
            txt = """<hr/>
                    <div style="text-align:center;width:{}px;height:{}px;
                           position:relative;background:black;margin-bottom:200px">
                           <h1 style="color:#fff;margin-bottom:300px">{}</h1>
                    </div>""".format(self.page_width, 50, 'Welcome')
            self.notification_div = {
                'top': Div(text=txt, width=self.page_width, height=20),
                'bottom': Div(text=txt, width=self.page_width, height=10),
            }
            lag_section_head_txt = 'Lag relationships between {} and...'.format(self.variable)

            self.section_divider = '-----------------------------------'
            self.section_headers = {

                'lag': self.section_header_div(text=lag_section_head_txt,
                                               width=600, html_header='h2', margin_top=5,
                                               margin_bottom=-155),
                'distribution': self.section_header_div(text='Pre-transform distribution:',
                                                        width=600, html_header='h2', margin_top=5,
                                                        margin_bottom=-155),
                'relationships': self.section_header_div(
                    text='Relationships between variables:{}'.format(self.section_divider),
                    width=600, html_header='h2', margin_top=5,
                    margin_bottom=-155),
                'correlations': self.section_header_div(
                    text='Correlations:',
                    width=600, html_header='h3', margin_top=5,
                    margin_bottom=-155),
                'forecast': self.section_header_div(text='Forecasts:{}'.format(self.section_divider),
                                                    width=600, html_header='h2', margin_top=5,
                                                    margin_bottom=-155),

            }

            # ----- UPDATED DIVS END

            # ----------------------  DIVS ----------------------------

        def section_header_div(self, text, html_header='h2', width=600, margin_top=150, margin_bottom=-150):
            text = """<div style="margin-top:{}px;margin-bottom:-{}px;"><{} style="color:#4221cc;">{}</{}></div>""" \
                .format(margin_top, margin_bottom, html_header, text, html_header)
            return Div(text=text, width=width, height=15)

        def notification_updater(self, text):
            txt = """<div style="text-align:center;background:black;width:100%;">
                    <h4 style="color:#fff;">
                    {}</h4></div>""".format(text)
            for key in self.notification_div.keys():
                self.notification_div[key].text = txt


        # //////////////  DIVS   /////////////////////////////////

        def title_div(self, text, width=700):
            text = '<h2 style="color:#4221cc;">{}</h2>'.format(text)
            return Div(text=text, width=width, height=15)

        def corr_information_div(self, width=400, height=300):
            div_style = """ 
                style='width:350px; margin-left:-600px;
                border:1px solid #ddd;border-radius:3px;background:#efefef50;' 
            """
            txt = """
            <div {}>
            <h4 {}>How to interpret relationships </h4>
            <ul style='margin-top:-10px;'>
                <li>
                Positive: as variable 1 increases, so does variable 2.
                </li>
                <li>
                Negative: as variable 1 increases, variable 2 decreases.
                </li>
                <li>
                Strength: decisions can be made on the basis of strong and moderate relationships.
                </li>
                <li>
                No relationship/not significant: no statistical support for decision making.
                </li>
                 <li>
               The scatter graphs (below) are useful for visual confirmation.
                </li>
                 <li>
               The histogram (right) shows the distribution of the variable.
                </li>
            </ul>
            </div>

            """.format(div_style, self.header_style)
            div = Div(text=txt, width=width, height=height)
            return div

        # /////////////////////////////////////////////////////////////

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
            try:
                df1 = df1[self.cols]

                for key,value in self.groupby_dict.items():
                    if value == 'count':
                        if self.select_values[key] != 'all':
                            df1 = df1[df1[key] == self.select_values[key]]
                return df1

            except Exception:
                logger.error('filter', exc_info=True)

        def prep_data(self, df):
            try:
                df = self.filter_df(df)
                # set up code columns
                codes = {
                    'gender' : {
                        'male' : 1,
                        'female' : 2,
                        'other' : 3
                    },
                    'status':{
                        'guest':1,
                        'member':2
                    }
                }
                for col in df.columns:
                    coded_col = col + '_coded'
                    if 'gender' in col:
                        df[coded_col] = df[col].map(codes['gender'])
                    if 'status' == col:
                        df[coded_col] = df[col].map(codes['status'])


                self.df = df.set_index(self.timestamp_col)
                # groupby and resample
                self.df1 = self.df.groupby('name').resample(self.resample_period).agg(self.groupby_dict)
                self.df1 = self.df1.reset_index()
                self.df1 = self.df1.fillna(0)


                logger.warning('LINE 288 df:%s',self.df1.head(10))

            except Exception:
                logger.error('prep data', exc_info=True)

        def tsa(self,launch):
            try:
                df = self.df.resample('D').agg({self.tsa_variable: 'mean'})
                df = df.reset_index()
                label = self.tsa_variable+'_diff'
                df[label] = df[self.tsa_variable].diff()
                df = df.fillna(0)

                rename = {self.timestamp_col:'ds',self.tsa_variable:'y'}
                df = df.rename(columns=rename)
                df = df[['ds','y']]
                logger.warning('df:%s',df.tail())
                m = Prophet()
                m.fit(df)

                future = m.make_future_dataframe(periods=self.forecast_days)
                forecast = m.predict(future)
                print(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail())
                print(list(forecast.columns))
                for idx,col in enumerate(['yhat', 'yhat_lower', 'yhat_upper']):
                    if idx == 0:
                        p = forecast.hvplot.line(x='ds',y=col,width=600,height=250,
                                                 value_label='$',legend=False).relabel(col)
                    else:
                        p *= forecast.hvplot.scatter(x='ds'
                                                     ,y=col,width=600,height=250,
                                                     value_label='$',legend=False).relabel(col)

                for idx,col in enumerate(['trend','weekly']):
                    if idx == 0:
                        q = forecast.hvplot.line(x='ds',y=col,width=550,height=250,
                                                 value_label='$',legend=False).relabel(col)
                    else:
                        q *= forecast.hvplot.line(x='ds',y=col,width=550,
                                                  height=250,value_label='$',legend=False).relabel(col)

                return p + q
            except Exception:
                logger.error("TSA:", exc_info=True)
    def update_variable(attr, old, new):
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.variable = new
        thistab.section_head_updater('lag', thistab.variable)
        thistab.trigger += 1
        stream_launch_matrix.event(launch=thistab.trigger)
        stream_launch_corr.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")


    def update_IVs(attrname, old, new):
        thistab.notification_updater("Calculations in progress! Please wait.")
        for item in thistab.select_values.keys():
            thistab.select_values[item] = thistab.select[item].value
        thistab.prep_data(thistab.df)
        thistab.trigger += 1
        stream_launch_matrix.event(launch=thistab.trigger)
        stream_launch_corr.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")


    def update(attrname, old, new):
        thistab.notification_updater("Calculations underway. Please be patient")
        thistab.df = thistab.pym.load_df(start_date=datepicker_start.value,
                                         end_date=datepicker_end.value,
                                         cols=[], table=thistab.table, timestamp_col=thistab.timestamp_col)

        thistab.df['gender_code'] = thistab.df['gender'].apply(
            lambda x: 1 if x == 'male' else 2)
        thistab.df1 = thistab.prep_data(thistab.df)
        thistab.trigger += 1
        stream_launch_matrix.event(launch=thistab.trigger)
        stream_launch_corr.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    def update_resample(attrname, old, new):
        thistab.notification_updater("Calculations underway. Please be patient")
        thistab.resample_period = new
        thistab.df1 = thistab.prep_data(thistab.df)
        thistab.trigger += 1
        stream_launch_matrix.event(launch=thistab.trigger)
        stream_launch_corr.event(launch=thistab.trigger)
        stream_launch.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    def update_lags_selected():
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.lag_days = lags_input.value
        logger.warning('line 381, new checkboxes: %s', thistab.lag_days)
        thistab.trigger += 1
        stream_launch_lags_var.event(launch=thistab.trigger)
        stream_launch.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    def update_multiline(attrname, old, new):
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.multiline_vars['x'] = multiline_x_select.value
        thistab.multiline_vars['y'] = multiline_y_select.value
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    def update_forecast(attrname, old, new):
        thistab.notification_updater("Calculations underway. Please be patient")
        thistab.forecast_days = int(select_forecast_days.value)
        thistab.tsa_variable = forecast_variable_select.value
        thistab.trigger += 1
        stream_launch_tsa.event(launch=thistab.trigger)
        thistab.notification_updater("ready")

    try:
        # SETUP
        table = 'bcc_composite'
        cols = cols_to_load['guest'] + cols_to_load['rental']
        thistab = Thistab(table, cols, [])

        # setup dates
        first_date_range = datetime.strptime("2013-04-25 00:00:00", "%Y-%m-%d %H:%M:%S")
        last_date_range = datetime.now().date()
        last_date = dashboard_config['dates']['last_date'] - timedelta(days=1)
        first_date = last_date - timedelta(days=1000)
        # initial function call
        thistab.df = thistab.pym.load_df(start_date=first_date,
                                         end_date=last_date,
                                         cols=[], table=thistab.table, timestamp_col=thistab.timestamp_col)

        thistab.prep_data(thistab.df)

        # MANAGE STREAM
        stream_launch_hist = streams.Stream.define('Launch', launch=-1)()
        stream_launch_matrix = streams.Stream.define('Launch_matrix', launch=-1)()
        stream_launch_corr = streams.Stream.define('Launch_corr', launch=-1)()
        stream_launch_lags_var = streams.Stream.define('Launch_lag_var', launch=-1)()
        stream_launch = streams.Stream.define('Launch', launch=-1)()
        stream_launch_tsa = streams.Stream.define('Launch_tsa', launch=-1)()


        # CREATE WIDGETS
        datepicker_start = DatePicker(title="Start", min_date=first_date_range,
                                      max_date=last_date_range, value=first_date)

        datepicker_end = DatePicker(title="End", min_date=first_date_range,
                                    max_date=last_date_range, value=last_date)

        variable_select = Select(title='Select variable',
                                 value=thistab.variable,
                                 options=thistab.variables)

        lag_variable_select = Select(title='Select lag variable',
                                     value=thistab.lag_variable,
                                     options=thistab.feature_list)

        lag_select = Select(title='Select lag',
                            value=str(thistab.lag),
                            options=thistab.lag_menu)

        select_forecast_days = Select(title='Select # of days which you want forecasted',
                                      value=str(thistab.forecast_days),
                                      options=['10', '20', '30', '40', '50', '60', '70', '80', '90'])

        forecast_variable_select = Select(title='Select forecast variable',
                                     value=thistab.menus['tsa'][0],
                                     options=thistab.menus['tsa'])




        resample_select = Select(title='Select resample period',
                                 value='D', options=['D', 'W', 'M', 'Q'])

        multiline_y_select = Select(title='Select comparative DV(y)',
                                    value=thistab.multiline_vars['y'],
                                    options=['price','amount', 'visit_duration'])

        multiline_x_select = Select(title='Select comparative IV(x)',
                                    value=thistab.multiline_vars['x'],
                                    options=['category','gender','rental_employee_gender','status','item'])

        lags_input = TextInput(value=thistab.lag_days, title="Enter lags (integer(s), separated by comma)",
                               height=55, width=300)
        lags_input_button = Button(label="Select lags, then click me!", width=10, button_type="success")

        # --------------------- PLOTS----------------------------------

        # tables
        hv_tsa = hv.DynamicMap(thistab.tsa, streams=[stream_launch_tsa])
        tsa = renderer.get_plot(hv_tsa)

        # setup divs

        # handle callbacks
        variable_select.on_change('value', update_variable)
        resample_select.on_change('value', update_resample)
        thistab.select['area'].on_change('value', update_IVs)
        thistab.select['gender'].on_change('value', update_IVs)
        thistab.select['rental_employee_gender'].on_change('value', update_IVs)
        thistab.select['item'].on_change('value', update_IVs)
        thistab.select['category'].on_change('value', update_IVs)
        thistab.select['status'].on_change('value', update_IVs)
        select_forecast_days.on_change('value',update_forecast)
        forecast_variable_select.on_change('value',update_forecast)
        datepicker_start.on_change('value', update)
        datepicker_end.on_change('value', update)


        multiline_x_select.on_change('value', update_multiline)
        multiline_y_select.on_change('value', update_multiline)

        # COMPOSE LAYOUT
        # put the controls in a single element
        controls_tsa = WidgetBox(
            datepicker_start, datepicker_end, variable_select,
            thistab.select['status'], resample_select,
            thistab.select['gender'], thistab.select['category'],
            thistab.select['area'],forecast_variable_select,
            select_forecast_days
        )


        # create the dashboards

        grid = gridplot([
            [thistab.notification_div['top']],
            [Spacer(width=20, height=70)],

            [thistab.section_headers['forecast']],
            [tsa.state, controls_tsa],
            [Spacer(width=20, height=30)],
            [thistab.notification_div['bottom']]
        ])

        # Make a tab with the layout
        tab = Panel(child=grid, title=panel_title)
        return tab

    except Exception:
        logger.error('EDA projects:', exc_info=True)
        return tab_error_flag(panel_title)
