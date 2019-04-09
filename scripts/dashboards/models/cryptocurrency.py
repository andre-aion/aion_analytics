from datetime import datetime, timedelta, date

import pydot
from bokeh.layouts import gridplot
from bokeh.models import Panel, Div, DatePicker, WidgetBox, Button, Select, TableColumn, ColumnDataSource, DataTable
from sklearn import metrics
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix, classification_report
from sklearn.model_selection import train_test_split
from sklearn.tree import export_graphviz
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline

from scripts.databases.pythonClickhouse import PythonClickhouse
from scripts.utils.dashboards.EDA.mytab_interface import Mytab
from scripts.utils.mylogger import mylogger
from scripts.utils.myutils import datetime_to_date
from scripts.streaming.streamingDataframe import StreamingDataframe as SD
from config.dashboard import config as dashboard_config


from tornado.gen import coroutine
from scipy.stats import linregress

from operator import itemgetter
import pandas as pd
import dask as dd
import holoviews as hv
from holoviews import streams

from scripts.utils.myutils import tab_error_flag
logger = mylogger(__file__)

hv.extension('bokeh', logo=False)
renderer = hv.renderer('bokeh')

table = 'crypto_modelling'
groupby_dict = {
    'watch': 'mean',
    'fork': 'mean',
    'issue': 'mean',
    'release': 'mean',
    'push': 'mean',
    'close': 'mean',
    'high': 'mean',
    'low': 'mean',
    'market_cap': 'mean',
    'volume': 'mean',
    'sp_volume':'mean',
    'sp_close':'mean',
    'russell_volume':'mean',
    'russell_close':'mean'
}

@coroutine
def cryptocurrency_tab(cryptos):
    lags_corr_src = ColumnDataSource(data=dict(
        Variable_1=[],
        Variable_2=[],
        Relationship=[],
        r=[],
        p_value=[]
    ))
    class Thistab(Mytab):
        def __init__(self, table, cols,dedup_cols=[]):
            Mytab.__init__(self, table, cols, dedup_cols)
            self.table = table
            self.cols = cols
            self.DATEFORMAT = "%Y-%m-%d %H:%M:%S"
            self.df = None
            self.df1 = {}  # to contain churned and retained splits
            self.df_predict = None
            self.day_diff = 1  # for normalizing for classification periods of different lengths
            self.df_grouped = ''

            self.cl = PythonClickhouse('aion')
            self.items = cryptos
            # add all the coins to the dict
            self.github_cols = ['watch','fork','issue','release','push']
            self.index_cols = ['close','high','low','market_cap','volume']

            self.trigger = 0
            txt = """<div style="text-align:center;background:black;width:100%;">
                                                                           <h1 style="color:#fff;">
                                                                           {}</h1></div>""".format('Welcome')
            self.notification_div = {
                'top': Div(text=txt, width=1400, height=20),
                'bottom':  Div(text=txt, width=1400, height=10)
            }

            self.groupby_dict = groupby_dict
            self.feature_list = list(self.groupby_dict.keys())
            self.variable = 'fork'
            self.crypto = 'all'
            self.lag_variable = 'push'
            self.lag_days = [10, 20, 30]
            self.lag = 0
            self.lag_menu = [str(x) for x in range(0,100)]


            self.strong_thresh = .65
            self.mod_thresh = 0.4
            self.weak_thresh = 0.25
            self.corr_df = None
            self.div_style = """ style='width:350px; margin-left:25px;
                                    border:1px solid #ddd;border-radius:3px;background:#efefef50;' 
                                    """

            self.header_style = """ style='color:blue;text-align:center;' """
            lag_section_head_txt = 'Lag relationships between {} and...'.format(self.variable)
            self.section_header_div = {
                'lag' : self.title_div(lag_section_head_txt, 400)
            }

        # ////////////////////////// HELPERs ///////////////////////
        def section_head_updater(self,section, txt):
            try:
                self.section_header_div[section].text = txt
            except Exception:
                logger.error('',exc_info=True)

        def notification_updater(self, text):
            txt = """<div style="text-align:center;background:black;width:100%;">
                    <h4 style="color:#fff;">
                    {}</h4></div>""".format(text)
            for key in self.notification_div.keys():
                self.notification_div[key].text = txt


        # ///////////////////////////////////////////////////////////

        def prep_data(self,df):
            try:
                # groupby
                #cols1 = list(self.groupby_dict.keys())
                #df = df[cols1]
                #df = df.groupby(['timestamp','crytpo']).agg(self.groupby_dict)
                df = df.fillna(0)
                self.df = df
                logger.warning('df:%s',self.df.head(10))

            except Exception:
                logger.error('prep data', exc_info=True)

        def lags_plot(self,launch):
            try:
                df = self.df.copy()
                df = df[[self.lag_variable,self.variable]]
                df = df.compute()
                cols = [self.lag_variable]
                for day in self.lag_days:
                    label = self.lag_variable + '_'+str(day)
                    df[label] = df[self.lag_variable].shift(day)
                    cols.append(label)
                df = df.dropna()
                self.lags_corr(df)
                # plot the comparison
                logger.warning('in lags plot: df:%s',df.head(10))
                return df.hvplot(x=self.variable,y=cols,kind='scatter',alpha=0.4)
            except Exception:
                logger.error('lags plot',exc_info=True)

        # calculate the correlation produced by the lags vector
        def lags_corr(self, df):
            try:
                corr_dict_data = {
                    'Variable_1': [],
                    'Variable_2': [],
                    'Relationship': [],
                    'r': [],
                    'p_value': []
                }
                a = df[self.variable].tolist()
                for col in df.columns:
                    if col not in ['timestamp',self.variable]:
                        b = df[col].tolist()
                        slope, intercept, rvalue, pvalue, txt = self.corr_label(a,b)
                        corr_dict_data['Variable_1'].append(self.variable)
                        corr_dict_data['Variable_2'].append(col)
                        corr_dict_data['Relationship'].append(txt)
                        corr_dict_data['r'].append(round(rvalue, 4))
                        corr_dict_data['p_value'].append(round(pvalue, 4))


                lags_corr_src.stream(corr_dict_data,rollover=(len(corr_dict_data)-1))
                columns = [
                    TableColumn(field="Variable_1", title="Variable_1"),
                    TableColumn(field="Variable_2", title="Variable_2"),
                    TableColumn(field="Relationship", title="Relationship"),
                    TableColumn(field="r", title="r"),
                    TableColumn(field="p_value", title="p_value"),

                ]
                data_table = DataTable(source=lags_corr_src, columns=columns, width=500, height=280)
                return data_table
            except Exception:
                logger.error('lags corr', exc_info=True)


        def correlation_table(self,launch):
            try:

                corr_dict = {
                    'Variable 1':[],
                    'Variable 2':[],
                    'Relationship':[],
                    'r':[],
                    'p-value':[]
                }

                df = self.corr_df
                if self.crypto != 'all':
                    df = df[df.crypto == self.crypto]
                #logger.warning('line df:%s',df.head(10))
                a = df[self.variable].tolist()
                for col in self.feature_list:
                    logger.warning('col :%s', col)
                    if col != self.variable:
                        logger.warning('%s:%s', col, self.variable)
                        if self.lag > 0:
                            df[col].shift(self.lag)
                            df = df.dropna()
                        b = df[col].tolist()
                        slope, intercept, rvalue, pvalue, txt = self.corr_label(a,b)
                        corr_dict['Variable 1'].append(self.variable)
                        corr_dict['Variable 2'].append(col)
                        corr_dict['Relationship'].append(txt)
                        corr_dict['r'].append(round(rvalue,4))
                        corr_dict['p-value'].append(round(pvalue,4))

                df = pd.DataFrame(
                    {
                        'Variable 1': corr_dict['Variable 1'],
                        'Variable 2': corr_dict['Variable 2'],
                        'Relationship': corr_dict['Relationship'],
                        'r':corr_dict['r'],
                        'p-value':corr_dict['p-value']

                     })
                #logger.warning('df:%s',df.head(23))
                return df.hvplot.table(columns=['Variable 1', 'Variable 2','Relationship','r','p-value'],
                                       width=550,height=400,title='Correlation between variables')
            except Exception:
                logger.error('correlation table', exc_info=True)

        def title_div(self, text, width=700):
            text = '<h2 style="color:#4221cc;">{}</h2>'.format(text)
            return Div(text=text, width=width, height=15)

        def corr_information_div(self, width=400, height=300):
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

            """.format(self.div_style, self.header_style)
            div = Div(text=txt, width=width, height=height)
            return div


        def hist(self,launch):
            try:
                return self.corr_df.hvplot.hist(
                    y=self.variable, bins=50, alpha=0.3,width=350,xaxis=False)
            except Exception:
                logger.warning('histogram', exc_info=True)

        def matrix_plot(self,launch=-1):
            try:
                logger.warning('line 306 self.feature list:%s',self.feature_list)

                df = self.df
                if self.crypto != 'all':
                    df = df[df.crypto == self.crypto]
                #df = df[self.feature_list]

                # get difference for money columns

                df = df.drop('timestamp',axis=1)
                df = df.fillna(0)
                df = df.compute()

                df = df.fillna(0)
                #logger.warning('line 302. df: %s',df.head(10))

                self.corr_df = df.copy()
                cols_temp = self.feature_list.copy()
                if self.variable in cols_temp:
                    cols_temp.remove(self.variable)
                #variable_select.options = cols_lst
                logger.warning('line 305 cols temp:%s',cols_temp)
                logger.warning('line 306 self.variable:%s',self.variable)

                p = df.hvplot.scatter(x=self.variable,y=cols_temp,width=400,
                                      subplots=True,shared_axes=False,xaxis=False).cols(3)

                return p

            except Exception:
                logger.error('matrix plot', exc_info=True)

    def update_variable(attr, old, new):
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.prep_data(thistab.df)
        thistab.variable = new
        thistab.section_head_updater('lag',thistab.variable)
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        stream_launch_matrix.event(launch=thistab.trigger)
        stream_launch_corr.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    def update_lag_plot_variable(attr, old, new):
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.prep_data(thistab.df)
        thistab.lag_variable = new
        thistab.trigger += 1
        stream_launch_lags_var.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")


    def update_crypto(attr, old, new):
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.prep_data(thistab.df)
        thistab.crypto = variable_select.value
        thistab.lag = int(lag_select.value)
        thistab.trigger += 1
        stream_launch_matrix.event(launch=thistab.trigger)
        stream_launch_corr.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    def update(attrname, old, new):
        thistab.notification_updater("Calculations underway. Please be patient")
        thistab.df_load(datepicker_start.value, datepicker_end.value,timestamp_col='timestamp')
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        stream_launch_matrix.event(launch=thistab.trigger)
        stream_launch_corr.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    try:
    # SETUP
        table = 'crypto_daily'
        cols = list(groupby_dict.keys()) + ['timestamp','crypto']
        thistab = Thistab(table,cols,[])

        # setup dates
        first_date_range = datetime.strptime("2018-04-25 00:00:00", "%Y-%m-%d %H:%M:%S")
        last_date_range = datetime.now().date()
        last_date = dashboard_config['dates']['last_date'] - timedelta(days=2)
        first_date = last_date - timedelta(days=300)
        # initial function call
        thistab.df_load(first_date, last_date,timestamp_col='timestamp')
        thistab.prep_data(thistab.df)

        # MANAGE STREAM
        # date comes out stream in milliseconds
        stream_launch = streams.Stream.define('Launch', launch=-1)()
        stream_launch_matrix = streams.Stream.define('Launch_matrix', launch=-1)()
        stream_launch_corr = streams.Stream.define('Launch_corr', launch=-1)()
        stream_launch_lags_var = streams.Stream.define('Launch_lag_var', launch=-1)()


    # CREATE WIDGETS
        datepicker_start = DatePicker(title="Start", min_date=first_date_range,
                                  max_date=last_date_range, value=first_date)

        datepicker_end = DatePicker(title="End", min_date=first_date_range,
                                max_date=last_date_range, value=last_date)

        variable_select = Select(title='Select variable',
                                 value='fork',
                                 options=thistab.feature_list)

        lag_variable_select = Select(title='Select lag variable',
                             value=thistab.lag_variable,
                             options=thistab.feature_list)

        lag_select = Select(title='Select lag',
                            value=str(thistab.lag),
                            options=thistab.lag_menu)

        crypto_select = Select(title='Select cryptocurrency',
                               value='all',
                               options=['all']+thistab.items)

        # --------------------- PLOTS----------------------------------
        columns = [
            TableColumn(field="Variable_1", title="Variable_1"),
            TableColumn(field="Variable_2", title="Variable_2"),
            TableColumn(field="Relationship", title="Relationship"),
            TableColumn(field="r", title="r"),
            TableColumn(field="p_value", title="p_value"),

        ]
        lags_corr_table = DataTable(source=lags_corr_src, columns=columns, width=500, height=280)


        width = 800

        hv_matrix_plot = hv.DynamicMap(thistab.matrix_plot,
                                       streams=[stream_launch_matrix])
        hv_corr_table = hv.DynamicMap(thistab.correlation_table,
                                      streams=[stream_launch_corr])
        hv_hist_plot = hv.DynamicMap(thistab.hist, streams=[stream_launch_corr])
        hv_lags_plot = hv.DynamicMap(thistab.lags_plot, streams=[stream_launch_lags_var])

        matrix_plot = renderer.get_plot(hv_matrix_plot)
        corr_table = renderer.get_plot(hv_corr_table)
        hist_plot = renderer.get_plot(hv_hist_plot)
        lags_plot = renderer.get_plot(hv_lags_plot)

        # setup divs


        # handle callbacks
        variable_select.on_change('value', update_variable)
        lag_variable_select.on_change('value', update_lag_plot_variable)
        lag_select.on_change('value',update)
        crypto_select.on_change('value', update_crypto)
        datepicker_start.on_change('value',update)
        datepicker_end.on_change('value',update)

        # COMPOSE LAYOUT
        # put the controls in a single element
        controls_left = WidgetBox(
            datepicker_start,
            variable_select,
            lag_select)

        controls_right = WidgetBox(
            datepicker_end,
            crypto_select)

        # create the dashboards

        grid = gridplot([
            [thistab.notification_div['top']],
            [controls_left, controls_right],
            [thistab.title_div('Relationships between variables', 400)],
            [corr_table.state, thistab.corr_information_div(), hist_plot.state],
            [matrix_plot.state],
            [thistab.section_header_div['lag'], lag_variable_select],
            [lags_plot.state, lags_corr_table],
            [thistab.notification_div['bottom']]

        ])

        # Make a tab with the layout
        tab = Panel(child=grid, title='Crypto')
        return tab

    except Exception:
        logger.error('crypto:', exc_info=True)
        return tab_error_flag('crypto')
