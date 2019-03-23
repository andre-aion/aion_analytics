from holoviews import streams

from scripts.utils.mylogger import mylogger
from scripts.utils.myutils import tab_error_flag, datetime_to_date
from scripts.utils.dashboards.EDA.mytab_interface import Mytab

from bokeh.layouts import gridplot, WidgetBox
from bokeh.models import Panel
from bokeh.models.widgets import Div, \
    DatePicker, Select

from datetime import datetime, timedelta

import holoviews as hv
from tornado.gen import coroutine
from numpy import inf
import pandas as pd
from scipy.stats import linregress

logger = mylogger(__file__)

hv.extension('bokeh', logo=False)
renderer = hv.renderer('bokeh')

variable_cols = [
    'value',
    'transaction_cost',
    'block_time',
    'balance',
    'difficulty',
    'mining_reward',
    'nrg_reward',
    'num_transactions',
    'hash_power',
    'russell_close',
    'russell_volume',
    'sp_close',
    'sp_volume'
]
menus = {
    'account_type' : ['all','contract','miner','native_user','token_user'],
    'update_type': ['all','contract_deployment','internal_transfer','mined_block','token_transfer','transaction'],
    'period' : ['D','W','M','H']
}

@coroutine
def account_activity_tab(DAYS_TO_LOAD=30):
    class Thistab(Mytab):
        def __init__(self, table,cols=[], dedup_cols=[]):
            Mytab.__init__(self, table, cols, dedup_cols)
            self.table = table
            self.cols = cols
            self.period = menus['period'][0]
            self.update_type = menus['update_type'][0]
            self.trigger = 0
            txt = """<div style="text-align:center;background:black;width:100%;">
                                                               <h1 style="color:#fff;">
                                                               {}</h1></div>""".format('Welcome')
            self.notification_div = Div(text=txt,width=1400,height=20)
            self.df_warehouse = None

            # correlation
            self.variable = 'block_time'
            self.strong_thresh = .65
            self.mod_thresh = 0.45
            self.weak_thresh = 0.25
            self.corr_df = None
            self.div_style = """ style='width:350px; margin-left:25px;
                        border:1px solid #ddd;border-radius:3px;background:#efefef50;' 
                        """

            self.header_style = """ style='color:blue;text-align:center;' """

        def clean_data(self, df):
            df = df.fillna(0)
            df[df == -inf] = 0
            df[df == inf] = 0
            return df

        def load_df(self,start_date, end_date):
            try:
                # make block_timestamp into index
                self.df_load(start_date, end_date)
                #logger.warning('df loaded:%s',self.df.head())
            except Exception:
                logger.warning('load df',exc_info=True)

        def prep_data(self):
            try:
                # make block_timestamp into index
                self.df1 = self.df.set_index('block_timestamp',sorted=True)

            except Exception:
                logger.warning('load df',exc_info=True)

        def plot_account_activity(self,launch=-1):
            try:
                if self.update_type == 'all':
                    df = self.df1[self.df1['value'] >= 0]
                else:
                    df = self.df1[(self.df1['value'] >= 0) & (self.df1['update_type'] == self.update_type)]
                df = df.resample(self.period).agg({'value':'sum','address':'count'})
                df = df.reset_index()
                df = df.compute()
                df = df.rename(index=str,columns={'address':'period_activity'})

                df['value_delta(%)'] = df['value'].pct_change(fill_method='ffill')
                df['value_delta(%)'] = df['value_delta(%)'].multiply(100)

                df['activity_delta(%)'] = df['period_activity'].pct_change(fill_method='ffill')
                df['activity_delta(%)'] = df['activity_delta(%)'].multiply(100)
                df = df.fillna(0)
                df = df.rename(index=str,columns={'value':'amount'})
                #logger.warning('df in balance after resample:%s',df.tail(10))

                # make block_timestamp into index
                return df.hvplot.line(x='block_timestamp', y=['amount','period_activity'],
                                      title='total value, # of transactions')+\
                       df.hvplot.line(x='block_timestamp', y=['value_delta(%)','activity_delta(%)'],
                                      title='# of transactions')
                # make block_timestamp into index
            except Exception:
                logger.warning('load df',exc_info=True)

        def plot_account_churned(self, launch=-1):
            try:
                logger.warning('df1 head:%s',self.df1.columns)
                if self.update_type == 'all':
                    df = self.df1[self.df1['balance'] <= 0]
                else:
                    df = self.df1[(self.df1['balance'] <= 0) & (self.df1['update_type'] == self.update_type)]

                df = df.resample(self.period).agg({'balance': 'count'})

                df = df.reset_index()
                df = df.compute()
                df['perc_change'] = df['balance'].pct_change(fill_method='ffill')
                df.perc_change = df.perc_change.multiply(100)
                df = df.fillna(0)
                # df = self.clean_data(df)

                # make block_timestamp into index
                return df.hvplot.line(x='block_timestamp', y=['balance'], value_label='# churned',
                                      title='accounts churned by period') + \
                       df.hvplot.line(x='block_timestamp', y=['perc_change'], value_label='%',
                                      title='percentage churned change by period')
            except Exception:
                logger.warning('load df', exc_info=True)

        def label_joined_churned(self,df):
            try:
                df['']
            except Exception:
                logger.warning('label joined churned', exc_info=True)

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
                logger.warning('line 173 df:%s',df.head(10))
                a = df[self.variable].tolist()
                for col in df.columns.tolist():
                    if col != self.variable:
                        logger.warning('%s:%s', col, self.variable)
                        b = df[col].tolist()
                        slope, intercept, rvalue, pvalue, std_err = linregress(a, b)
                        #logger.warning('slope:%s,intercept:%s,rvalue:%s,pvalue:%s,std_err:%s',
                        #             slope, intercept, rvalue, pvalue, std_err)
                        if pvalue < 0.05:
                            if abs(rvalue) <= self.weak_thresh:
                                txt = 'none'
                            else:
                                strength = 'weak'
                                if rvalue > 0:
                                    direction = 'positive'
                                if rvalue < 0:
                                    direction = 'negative'
                                if abs(rvalue) > self.mod_thresh:
                                    strength = 'moderate'
                                if abs(rvalue) > self.strong_thresh:
                                    strength = 'strong'

                                txt = "{} {}".format(strength,direction)
                        else:
                            txt = 'Not significant'
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
                return df.hvplot.table(columns=['Variable 1', 'Variable 2','Relationship','r','p-value'],
                                       width=550,height=400,title='Correlation between variables')
            except Exception:
                logger.warning('correlation table', exc_info=True)


        def matrix_plot(self,launch=-1):
            try:
                if self.update_type != 'all':
                    df = self.df1[self.df1['update_type'] == self.update_type]
                else:
                    df = self.df1
                groupby_dict = {
                    'difficulty':'mean',
                    'nrg_reward':'mean',
                    'num_transactions':'mean',
                    'block_time':'mean',
                    'transaction_cost':'mean',
                    'transaction_value':'mean',
                    'balance':'mean',
                    'hash_power':'mean',
                    'mining_reward':'mean',
                    'russell_close':'mean',
                    'sp_close':'mean',
                    'russell_volume':'mean',
                    'sp_volume':'mean'}
                df = df.rename(columns={'value':'transaction_value'}) #cannot have a column named value
                if 'value' in variable_cols:
                    variable_cols.remove('value')
                    variable_cols.append('transaction_value')
                df = df[variable_cols]

                # get difference for money columns

                df = df.resample(self.period).agg(groupby_dict)
                df = df.reset_index()
                df = df.drop('block_timestamp',axis=1)
                df = df.fillna(0)
                df = df.compute()
                df['russell_close'] = df['russell_close'].diff()
                df['sp_close'] = df['sp_close'].diff()
                df['sp_volume'] = df['sp_volume'].diff()
                df['russell_volume'] = df['russell_volume'].diff()
                df = df.fillna(0)

                self.corr_df = df.copy()
                cols_lst = df.columns.tolist()
                cols_temp = cols_lst.copy()
                cols_temp.remove(self.variable)
                variable_select.options = cols_lst
                logger.warning('line 308 ;%s:%s',self.variable,cols_temp)
                logger.warning('line 309. df: %s',df.head(10))
                p = df.hvplot.scatter(x=self.variable,y=cols_temp,width=400,
                                      subplots=True,shared_axes=False,xaxis=False).cols(3)

                return p

            except Exception:
                logger.error('matrix plot', exc_info=True)


    def update(attrname, old, new):
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.load_df(datepicker_start.value,datepicker_end.value)
        thistab.prep_data()
        thistab.update_type = event_select.value
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        stream_launch_matrix.event(launch=thistab.trigger)
        thistab.notification_updater("Ready.")

    def update_resample(attr,old,new):
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.prep_data()
        thistab.period = period_select.value
        thistab.update_type = event_select.value
        thistab.variable = variable_select.value
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        stream_launch_matrix.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    def update_event(attr, old, new):
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.prep_data()
        thistab.update_type = event_select.value
        thistab.variable = variable_select.value
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        stream_launch_matrix.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    def update_variable(attr, old, new):
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.variable = new
        thistab.trigger += 1
        stream_launch_matrix.event(launch=thistab.trigger)
        stream_launch_corr.event(launch=thistab.trigger)

        thistab.notification_updater("Ready!")

    try:
        cols = list(set(variable_cols + ['address','block_timestamp','update_type','account_type']))
        thistab = Thistab(table='account_external_warehouse',cols=cols)
        # STATIC DATES
        # format dates
        first_date_range = "2019-01-23 00:00:00"
        first_date_range = datetime.strptime(first_date_range, "%Y-%m-%d %H:%M:%S")
        last_date_range = datetime.now().date()
        last_date = datetime.now().date()
        first_date = datetime_to_date(last_date - timedelta(days=DAYS_TO_LOAD))

        thistab.load_df(first_date, last_date)
        thistab.prep_data()

        # MANAGE STREAM
        # date comes out stream in milliseconds
        stream_launch = streams.Stream.define('Launch',launch=-1)()
        stream_launch_matrix = streams.Stream.define('Launch_matrix',launch=-1)()
        stream_launch_corr = streams.Stream.define('Launch_corr',launch=-1)()


        # CREATE WIDGETS
        datepicker_start = DatePicker(title="Start", min_date=first_date_range,
                                      max_date=last_date_range, value=first_date)
        datepicker_end = DatePicker(title="End", min_date=first_date_range,
                                    max_date=last_date_range, value=last_date)

        period_select = Select(title='Select aggregation period',
                               value='day',
                               options=menus['period'])
        event_select = Select(title='Select transfer type',
                               value='all',
                               options=menus['update_type'])
        variable_select = Select(title='Select variable',
                              value='block_time',
                              options=variable_cols)

        # --------------------- PLOTS----------------------------------
        width = 800
        hv_account_churned = hv.DynamicMap(thistab.plot_account_churned,
                                           streams=[stream_launch]).opts(plot=dict(width=width, height=400))
        hv_account_activity = hv.DynamicMap(thistab.plot_account_activity,
                                            streams=[stream_launch]).opts(plot=dict(width=width, height=400))
        hv_matrix_plot = hv.DynamicMap(thistab.matrix_plot,
                                       streams=[stream_launch_matrix])
        hv_corr_table = hv.DynamicMap(thistab.correlation_table,
                                      streams=[stream_launch_corr])
        hv_hist_plot = hv.DynamicMap(thistab.hist,streams=[stream_launch_corr])

        account_churned = renderer.get_plot(hv_account_churned)
        account_activity = renderer.get_plot(hv_account_activity)
        matrix_plot = renderer.get_plot(hv_matrix_plot)
        corr_table = renderer.get_plot(hv_corr_table)
        hist_plot = renderer.get_plot(hv_hist_plot)

        # handle callbacks
        datepicker_start.on_change('value', update)
        datepicker_end.on_change('value', update)
        period_select.on_change('value',update_resample)
        event_select.on_change('value',update_event)
        variable_select.on_change('value',update_variable)


        # COMPOSE LAYOUT
        # put the controls in a single element
        controls_left = WidgetBox(
            datepicker_start,
            period_select)

        controls_right = WidgetBox(
            datepicker_end,
            event_select)

        # create the dashboards
        grid = gridplot([
            [thistab.notification_div],
            [controls_left, controls_right],
            [account_churned.state],
            [account_activity.state],
            [thistab.title_div('Relationships between variables', 400),variable_select],
            [corr_table.state, thistab.corr_information_div(),hist_plot.state],
            [matrix_plot.state]
            ])

        # Make a tab with the layout
        tab = Panel(child=grid, title='Account activity')
        return tab

    except Exception:
        logger.error('rendering err:', exc_info=True)
        return tab_error_flag('account activity')
