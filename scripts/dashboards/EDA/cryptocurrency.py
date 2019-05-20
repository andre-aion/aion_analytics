from datetime import datetime, timedelta, date

import pydot
from bokeh.layouts import gridplot
from bokeh.models import Panel, Div, DatePicker, WidgetBox, Button, Select, TableColumn, ColumnDataSource, DataTable, \
    Spacer
from sklearn import metrics
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix, classification_report
from sklearn.model_selection import train_test_split
from sklearn.tree import export_graphviz
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from scipy.stats import mannwhitneyu

from scripts.databases.pythonClickhouse import PythonClickhouse
from scripts.utils.dashboards.EDA.mytab_interface import Mytab
from scripts.utils.mylogger import mylogger
from scripts.utils.myutils import datetime_to_date
from scripts.streaming.streamingDataframe import StreamingDataframe as SD
from config.dashboard import config as dashboard_config
from bokeh.models.widgets import CheckboxGroup, TextInput

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
    'russell_close':'mean',
    'twu_tweets':'sum',
    'twu_mentions':'sum',
    'twu_positive': 'mean',
    'twu_compound':'mean',
    'twu_neutral':'mean',
    'twu_negative':'mean',
    'twu_emojis_positive': 'mean',
    'twu_emojis_compound': 'mean',
    'twu_emojis_neutral': 'mean',
    'twu_emojis_negative': 'mean',
    'twu_emojis':'sum',
    'twu_favorites': 'sum',
    'twu_retweets':'sum',
    'twu_hashtags': 'sum',
    'twu_replies':'sum',
    'twr_tweets':'sum',
    'twr_mentions':'sum',
    'twr_positive': 'mean',
    'twr_compound':'mean',
    'twr_neutral':'mean',
    'twr_negative':'mean',
    'twr_emojis_positive': 'mean',
    'twr_emojis_compound': 'mean',
    'twr_emojis_neutral': 'mean',
    'twr_emojis_negative': 'mean',
    'twr_emojis':'sum',
    'twr_favorites': 'sum',
    'twr_retweets':'sum',
    'twr_hashtags': 'sum',
    'twr_replies':'sum',
}

@coroutine
def cryptocurrency_eda_tab(cryptos,panel_title):
    lags_corr_src = ColumnDataSource(data=dict(
        variable_1=[],
        variable_2=[],
        relationship=[],
        lag=[],
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
            self.df1 = None
            self.df_predict = None
            self.day_diff = 1  # for normalizing for classification periods of different lengths
            self.df_grouped = ''

            self.cl = PythonClickhouse('aion')
            self.items = cryptos
            # add all the coins to the dict
            self.github_cols = ['watch','fork','issue','release','push']
            self.index_cols = ['close','high','low','market_cap','volume']

            self.trigger = 0

            self.groupby_dict = groupby_dict
            self.feature_list = list(self.groupby_dict.keys())
            self.variable = 'fork'
            self.crypto = 'all'
            self.lag_variable = 'push'
            self.lag_days = "1,2,3"
            self.lag = 0
            self.lag_menu = [str(x) for x in range(0,100)]


            self.strong_thresh = .65
            self.mod_thresh = 0.4
            self.weak_thresh = 0.25
            self.corr_df = None
            self.div_style =  """ 
                            style='width:350px; margin-left:-600px;
                            border:1px solid #ddd;border-radius:3px;background:#efefef50;' 
                        """

            self.header_style = """ style='color:blue;text-align:center;' """
            # track variable for AI for significant effects
            self.adoption_variables = {
                'user':[],
                'developer':['watch','fork']
            }

            self.significant_effect_dict = {}
            self.reset_adoption_dict(self.variable)
            self.relationships_to_check = ['weak','moderate','strong']
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
            #self.lag_section_head_txt = 'Lag relationships between {} and...'.format(self.variable)
            self.lag_section_head_txt = 'Lag relationships:'
            self.section_divider = '-----------------------------------'
            self.section_headers = {

                'lag': self.section_header_div(text=self.lag_section_head_txt,
                                               width=600, html_header='h3', margin_top=5,
                                               margin_bottom=-155),
                'distribution': self.section_header_div(
                    text='Pre transform distribution:{}'.format(self.section_divider),
                    width=600, html_header='h2', margin_top=5,
                    margin_bottom=-155),
                'relationships': self.section_header_div(
                    text='Relationships between variables:'.format(self.section_divider),
                    width=600, html_header='h2', margin_top=5,
                    margin_bottom=-155),
                'correlations': self.section_header_div(
                    text='non linear relationships between variables:',
                    width=600, html_header='h3', margin_top=5,
                    margin_bottom=-155),
                'non_linear': self.section_header_div(
                    text='non linear relationships between variables:',
                    width=600, html_header='h3', margin_top=5,
                    margin_bottom=-155),
            }

            # ----------------------  DIVS ----------------------------

        def section_header_div(self, text, html_header='h2', width=600, margin_top=150, margin_bottom=-150):
            text = """<div style="margin-top:{}px;margin-bottom:-{}px;"><{} style="color:#4221cc;">{}</{}></div>""" \
                .format(margin_top, margin_bottom, html_header, text, html_header)
            return Div(text=text, width=width, height=15)

        def notification_updater(self, text):
            txt = """<div style="text-align:center;background:black;width:{}px;">
                           <h4 style="color:#fff;">
                           {}</h4></div>""".format(self.page_width,text)
            for key in self.notification_div.keys():
                self.notification_div[key].text = txt

        def reset_adoption_dict(self, variable):
            self.significant_effect_dict[variable] = []

        def section_header_updater(self,text,section,html_header='h3', margin_top=150, margin_bottom=-150):
            text = """<div style="margin-top:{}px;margin-bottom:-{}px;"><{} style="color:#4221cc;">{}</{}></div>""" \
                .format(margin_top, margin_bottom, html_header, text, html_header)
            self.section_headers[section].text = text

        # //////////////  DIVS   /////////////////////////////////

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

        # /////////////////////////////////////////////////////////////
        def prep_data(self,df1):
            try:
                self.cols = list(df1.columns)

                df1['timestamp'] = df1['timestamp'].astype('M8[us]')
                df = df1.set_index('timestamp')
                #logger.warning('LINE 195 df:%s',df.head())
                # handle lag for all variables
                if self.crypto != 'all':
                    df = df[df.crypto == self.crypto]
                df = df.compute()
                #logger.warning('LINE 199: length before:%s',len(df))
                df = df.groupby('crypto').resample(self.resample_period).agg(self.groupby_dict)
                #logger.warning('LINE 201: length after:%s',len(df))

                df = df.reset_index()
                vars = self.feature_list.copy()
                if int(self.lag) > 0:
                    for var in vars:
                        if self.variable != var:
                            df[var] = df[var].shift(int(self.lag))
                df = df.dropna()
                self.df1 = df
                #logger.warning('line 184- prep data: df:%s',self.df.head(10))

            except Exception:
                logger.error('prep data', exc_info=True)


        def set_groupby_dict(self):
            try:
                pass
                
            except Exception:
                logger.error('set groupby dict', exc_info=True)

        #   ///////////////// PLOTS /////////////////////

        def lags_plot(self,launch):
            try:
                df = self.df.copy()
                df = df[[self.lag_variable,self.variable]]
                df = df.compute()
                cols = [self.lag_variable]
                lags = self.lag_days.split(',')
                for day in lags:
                    try:
                        label = self.lag_variable + '_' + day
                        df[label] = df[self.lag_variable].shift(int(day))
                        cols.append(label)
                    except:
                        logger.warning('%s is not an integer',day)
                df = df.dropna()
                self.lags_corr(df)
                # plot the comparison
                #logger.warning('in lags plot: df:%s',df.head(10))
                return df.hvplot(x=self.variable,y=cols,kind='scatter',alpha=0.4)
            except Exception:
                logger.error('lags plot',exc_info=True)

        # calculate the correlation produced by the lags vector
        def lags_corr(self, df):
            try:
                corr_dict_data = {
                    'variable_1': [],
                    'variable_2': [],
                    'relationship': [],
                    'lag':[],
                    'r': [],
                    'p_value': []
                }
                a = df[self.variable].tolist()
                for col in df.columns:
                    if col not in ['timestamp',self.variable]:
                        # find lag
                        var = col.split('_')
                        try:
                            tmp = int(var[-1])
   
                            lag = tmp
                        except Exception:
                            lag = 'None'

                        b = df[col].tolist()
                        slope, intercept, rvalue, pvalue, txt = self.corr_label(a,b)
                        corr_dict_data['variable_1'].append(self.variable)
                        corr_dict_data['variable_2'].append(col)
                        corr_dict_data['relationship'].append(txt)
                        corr_dict_data['lag'].append(lag)
                        corr_dict_data['r'].append(round(rvalue, 4))
                        corr_dict_data['p_value'].append(round(pvalue, 4))


                lags_corr_src.stream(corr_dict_data,rollover=(len(corr_dict_data['lag'])))
                columns = [
                    TableColumn(field="variable_1", title="variable 1"),
                    TableColumn(field="variable_2", title="variable 2"),
                    TableColumn(field="relationship", title="relationship"),
                    TableColumn(field="lag", title="lag(days)"),
                    TableColumn(field="r", title="r"),
                    TableColumn(field="p_value", title="p_value"),

                ]
                data_table = DataTable(source=lags_corr_src, columns=columns, width=900, height=400)
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
                # prep df
                df = self.df1
                # get difference for money columns
                df = df.drop('timestamp', axis=1)
                #df = df.compute()

                a = df[self.variable].tolist()

                for col in self.feature_list:
                    if col != self.variable:
                        #logger.warning('%s:%s', col, self.variable)
                        b = df[col].tolist()
                        slope, intercept, rvalue, pvalue, txt = self.corr_label(a,b)
                        # add to dict
                        corr_dict['Variable 1'].append(self.variable)
                        corr_dict['Variable 2'].append(col)
                        corr_dict['Relationship'].append(txt)
                        corr_dict['r'].append(round(rvalue,4))
                        corr_dict['p-value'].append(round(pvalue,4))

                        # update significant effect variables
                        if self.variable in self.adoption_variables['developer']:
                            if any(relationship in txt for relationship in self.relationships_to_check):
                                if self.variable not in self.significant_effect_dict.keys():
                                    self.significant_effect_dict[self.variable] = []
                                self.significant_effect_dict[self.variable].append(col)

                if self.variable in self.adoption_variables['developer']:
                    tmp = self.significant_effect_dict[self.variable].copy()
                    tmp = list(set(tmp))
                    tmp_dct = {
                        'features': tmp,
                        'timestamp': datetime.now().strftime(self.DATEFORMAT)
                    }
                    # write to redis
                    save_params = 'adoption_features:developer'+'-'+self.variable
                    self.redis.save(tmp_dct,
                                    save_params,
                                    "", "", type='checkpoint')

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


        def non_parametric_relationship_table(self,launch):
            try:

                corr_dict = {
                    'Variable 1':[],
                    'Variable 2':[],
                    'Relationship':[],
                    'stat':[],
                    'p-value':[]
                }
                # prep df
                df = self.df1
                # get difference for money columns
                df = df.drop('timestamp', axis=1)
                #df = df.compute()

                #logger.warning('line df:%s',df.head(10))
                a = df[self.variable].tolist()
                for col in self.feature_list:
                    if col != self.variable:
                        #logger.warning('%s:%s', col, self.variable)
                        b = df[col].tolist()
                        stat, pvalue, txt = self.mann_whitneyu_label(a,b)
                        corr_dict['Variable 1'].append(self.variable)
                        corr_dict['Variable 2'].append(col)
                        corr_dict['Relationship'].append(txt)
                        corr_dict['stat'].append(round(stat,4))
                        corr_dict['p-value'].append(round(pvalue,4))

                df = pd.DataFrame(
                    {
                        'Variable 1': corr_dict['Variable 1'],
                        'Variable 2': corr_dict['Variable 2'],
                        'Relationship': corr_dict['Relationship'],
                        'stat':corr_dict['stat'],
                        'p-value':corr_dict['p-value']

                     })
                #logger.warning('df:%s',df.head(23))
                return df.hvplot.table(columns=['Variable 1', 'Variable 2','Relationship','stat','p-value'],
                                       width=550,height=400,title='Non parametricrelationship between variables')
            except Exception:
                logger.error('non parametric table', exc_info=True)


        def hist(self,launch):
            try:

                return self.df.hvplot.hist(
                    y=self.feature_list,subplots=True,shared_axes=False,
                    bins=25, alpha=0.3,width=300).cols(4)
            except Exception:
                logger.warning('histogram', exc_info=True)

        def matrix_plot(self,launch=-1):
            try:
                logger.warning('line 306 self.feature list:%s',self.feature_list)

                df = self.df1

                #df = df[self.feature_list]

                # get difference for money columns

                #thistab.prep_data(thistab.df)
                if 'timestamp' in df.columns:
                    df = df.drop('timestamp',axis=1)
                #df = df.repartition(npartitions=1)
                #df = df.compute()

                df = df.fillna(0)
                #logger.warning('line 302. df: %s',df.head(10))

                cols_temp = self.feature_list.copy()
                if self.variable in cols_temp:
                    cols_temp.remove(self.variable)
                #variable_select.options = cols_lst

                p = df.hvplot.scatter(x=self.variable,y=cols_temp,width=330,
                                      subplots=True,shared_axes=False,xaxis=False).cols(4)

                return p

            except Exception:
                logger.error('matrix plot', exc_info=True)

        '''
        def regression(self,df):
            try:

            except Exception:
                logger.error('matrix plot', exc_info=True)
        '''
    def update_variable(attr, old, new):
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.prep_data(thistab.df)
        thistab.variable = new
        if thistab.variable in thistab.adoption_variables['developer']:
            thistab.reset_adoption_dict(thistab.variable)
        thistab.lag_section_head_txt = 'Lag relationships between {} and...'.format(thistab.variable)
        #thistab.section_header_updater('lag',thistab.lag_section_head_txt)
        thistab.trigger += 1
        stream_launch_matrix.event(launch=thistab.trigger)
        stream_launch_corr.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    def update_lag_plot_variable(attr, old, new):
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.lag_variable = new
        thistab.prep_data(thistab.df)
        thistab.trigger += 1
        stream_launch_lags_var.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    def update_crypto(attr, old, new):
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.crypto = crypto_select.value
        thistab.lag = int(lag_select.value)
        thistab.prep_data(thistab.df)
        thistab.trigger += 1
        stream_launch_matrix.event(launch=thistab.trigger)
        stream_launch_corr.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    def update_lag(attr, old, new):  # update lag & cryptocurrency
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.lag = int(lag_select.value)
        thistab.prep_data(thistab.df)
        thistab.trigger += 1
        stream_launch_matrix.event(launch=thistab.trigger)
        stream_launch_corr.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    def update(attrname, old, new):
        thistab.notification_updater("Calculations underway. Please be patient")
        thistab.df_load(datepicker_start.value, datepicker_end.value,timestamp_col='timestamp')
        thistab.prep_data(thistab.df)
        thistab.trigger += 1
        stream_launch_matrix.event(launch=thistab.trigger)
        stream_launch_corr.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    def update_resample(attrname, old, new):
        thistab.notification_updater("Calculations underway. Please be patient")
        thistab.resample_period = new
        thistab.prep_data(thistab.df)
        thistab.trigger += 1
        stream_launch_matrix.event(launch=thistab.trigger)
        stream_launch_corr.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    def update_lags_selected():
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.lag_days = lags_input.value
        logger.warning('line 381, new checkboxes: %s',thistab.lag_days)
        thistab.trigger += 1
        stream_launch_lags_var.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    try:
    # SETUP
        table = 'external_daily'
        cols = list(groupby_dict.keys()) + ['timestamp','crypto']
        thistab = Thistab(table,[],[])

        # setup dates
        first_date_range = datetime.strptime("2018-04-25 00:00:00", "%Y-%m-%d %H:%M:%S")
        last_date_range = datetime.now().date()
        last_date = dashboard_config['dates']['last_date'] - timedelta(days=2)
        first_date = last_date - timedelta(days=200)
        # initial function call
        thistab.df_load(first_date, last_date,timestamp_col='timestamp')
        thistab.prep_data(thistab.df)

        # MANAGE STREAM
        # date comes out stream in milliseconds
        #stream_launch_hist = streams.Stream.define('Launch', launch=-1)()
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

        resample_select = Select(title='Select resample period',
                                value='D',options=['D','W','M','Q'])

        lags_input = TextInput(value=thistab.lag_days, title="Enter lags (integer(s), separated by comma)",
                               height=55,width=300)
        lags_input_button = Button(label="Select lags, then click me!",width=10,button_type="success")

        # --------------------- PLOTS----------------------------------
        columns = [
            TableColumn(field="variable_1", title="variable 1"),
            TableColumn(field="variable_2", title="variable 2"),
            TableColumn(field="relationship", title="relationship"),
            TableColumn(field="lag", title="lag(days)"),
            TableColumn(field="r", title="r"),
            TableColumn(field="p_value", title="p_value"),

        ]
        lags_corr_table = DataTable(source=lags_corr_src, columns=columns, width=500, height=280)


        width = 800

        hv_matrix_plot = hv.DynamicMap(thistab.matrix_plot,
                                       streams=[stream_launch_matrix])
        hv_corr_table = hv.DynamicMap(thistab.correlation_table,
                                      streams=[stream_launch_corr])
        hv_nonpara_table = hv.DynamicMap(thistab.non_parametric_relationship_table,
                                     streams=[stream_launch_corr])
        #hv_hist_plot = hv.DynamicMap(thistab.hist, streams=[stream_launch_hist])
        hv_lags_plot = hv.DynamicMap(thistab.lags_plot, streams=[stream_launch_lags_var])

        matrix_plot = renderer.get_plot(hv_matrix_plot)
        corr_table = renderer.get_plot(hv_corr_table)
        nonpara_table = renderer.get_plot(hv_nonpara_table)
        lags_plot = renderer.get_plot(hv_lags_plot)

        # setup divs


        # handle callbacks
        variable_select.on_change('value', update_variable)
        lag_variable_select.on_change('value', update_lag_plot_variable)
        lag_select.on_change('value',update_lag)   # individual lag
        resample_select.on_change('value',update_resample)
        crypto_select.on_change('value', update_crypto)
        datepicker_start.on_change('value',update)
        datepicker_end.on_change('value',update)
        lags_input_button.on_click(update_lags_selected) # lags array

        # COMPOSE LAYOUT
        # put the controls in a single element
        controls = WidgetBox(
            datepicker_start,
            datepicker_end,
            variable_select,
            lag_select,
            crypto_select,
            resample_select)

        controls_lag = WidgetBox(
            lag_variable_select,
            lags_input,
            lags_input_button
        )

        # create the dashboards

        grid = gridplot([
            [thistab.notification_div['top']],
            [Spacer(width=20, height=70)],
            [matrix_plot.state,controls],
            [thistab.section_headers['relationships']],
            [Spacer(width=20, height=30)],
            [thistab.section_headers['correlations']],
            [Spacer(width=20, height=30)],
            [corr_table.state, thistab.corr_information_div()],
            [thistab.section_headers['non_linear']],
            [Spacer(width=20, height=30)],
            [nonpara_table.state],
            [thistab.section_headers['lag']],
            [Spacer(width=20, height=30)],
            [lags_plot.state,controls_lag],
            [lags_corr_table],
            [thistab.notification_div['bottom']]

        ])

        # Make a tab with the layout
        tab = Panel(child=grid, title=panel_title)
        return tab

    except Exception:
        logger.error('crypto:', exc_info=True)
        return tab_error_flag(panel_title)
