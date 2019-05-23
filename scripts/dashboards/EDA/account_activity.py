import gc

from holoviews import streams

from scripts.databases.pythonMongo import PythonMongo
from scripts.utils.mylogger import mylogger
from scripts.utils.myutils import tab_error_flag, datetime_to_date
from scripts.utils.interfaces.mytab_interface import Mytab

from bokeh.layouts import gridplot, WidgetBox
from bokeh.models import Panel, Spacer
from bokeh.models.widgets import Div, \
    DatePicker, Select

from datetime import datetime, timedelta

import holoviews as hv
from tornado.gen import coroutine
from numpy import inf
import pandas as pd
from scipy.stats import linregress
from config.hyp_variables import groupby_dict
from config.dashboard import config as dashboard_config

logger = mylogger(__file__)

hv.extension('bokeh', logo=False)
renderer = hv.renderer('bokeh')

table = 'accounts_predictive'


menus = {
    'account_type' : ['all','contract','miner','native_user','token_user'],
    'update_type': ['all','contract_deployment','internal_transfer','mined_block','token_transfer','transaction'],
    'period' : ['D','W','M','H'],
    'status' : ['all','active','churned','joined']
}

price_labels = ['open', 'low', 'market_cap', 'high', 'volume', 'close']

twitter_cols = ['twu_tweets', 'twu_mentions', 'twu_positive', 'twu_compound', 'twu_neutral',
                     'twu_negative', 'twu_emojis_positive', 'twu_emojis_compound', 'twu_emojis_neutral',
                     'twu_emojis_negative', 'twu_emojis', 'twu_favorites', 'twu_retweets', 'twu_hashtags',
                     'twu_replies',
                     'twr_tweets', 'twr_mentions', 'twr_positive', 'twr_compound', 'twr_neutral',
                     'twr_negative', 'twr_emojis_positive', 'twr_emojis_compound', 'twr_emojis_neutral',
                     'twr_emojis_negative', 'twr_emojis', 'twr_favorites', 'twr_retweets', 'twr_hashtags',
                     'twr_replies']

external_hourly = ['fork', 'release', 'push', 'watch', 'issue'] + twitter_cols
groupby_dict = {}
for crypto in ['aion','bitcoin','ethereum']:
    for item in price_labels + external_hourly:
        label = crypto + '_' + item
        if item in [
            'twu_tweets', 'twu_mentions','twu_emojis', 'twu_favorites', 'twu_retweets', 'twu_hashtags','twu_replies',
            'twr_tweets', 'twr_mentions','twr_emojis', 'twr_favorites', 'twr_retweets', 'twr_hashtags','twr_replies',
            'fork', 'release', 'push', 'watch', 'issue']:
            groupby_dict[label] = 'sum'
        else:
            groupby_dict[label] = 'mean'

for item in ['amount','transaction_cost','block_time','balance','difficulty',
             'mining_reward','nrg_reward','num_transactions','hash_power']:
    groupby_dict[item] = 'mean'

hyp_variables= list(groupby_dict.keys())

@coroutine
def account_activity_tab(DAYS_TO_LOAD=90,panel_title=None):
    class Thistab(Mytab):
        def __init__(self, table,cols=[], dedup_cols=[]):
            Mytab.__init__(self, table, cols, dedup_cols,panel_title=panel_title)
            self.table = table
            self.cols = cols
            self.period = menus['period'][0]

            self.update_type = menus['update_type'][0]
            self.status = menus['status'][0]
            self.account_type = menus['account_type'][0]


            self.trigger = 0

            self.df_warehouse = None

            # correlation
            self.variable = 'aion_fork'

            self.strong_thresh = .65
            self.mod_thresh = 0.4
            self.weak_thresh = 0.25
            self.corr_df = None
            self.div_style = """ style='width:350px; margin-left:25px;
                        border:1px solid #ddd;border-radius:3px;background:#efefef50;' 
                        """

            self.header_style = """ style='color:blue;text-align:center;' """
            self.feature_list = hyp_variables.copy()
            self.groupby_dict = groupby_dict
            self.pym = PythonMongo('aion')

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
                'account activity': self.section_header_div(text='Account activity:{}'.format(self.section_divider),
                                               width=600, html_header='h2', margin_top=5, margin_bottom=-155),
                'relationships': self.section_header_div(text='Relationships:{}'.format(self.section_divider),
                                                 width=600, html_header='h2', margin_top=5, margin_bottom=-155),
            }

        # ----------------------  DIVS ----------------------------
        def section_header_div(self, text, html_header='h2', width=600, margin_top=150, margin_bottom=-150):
            text = """<div style="margin-top:{}px;margin-bottom:-{}px;"><{} style="color:#4221cc;">{}</{}></div>""" \
                .format(margin_top, margin_bottom, html_header, text, html_header)
            return Div(text=text, width=width, height=15)

        def clean_data(self, df):
            df = df.fillna(0)
            df[df == -inf] = 0
            df[df == inf] = 0
            return df

        def load_df(self,start_date, end_date,cols,timestamp_col):
            try:
                # make timestamp into index
                self.df_load(start_date, end_date,cols=cols,timestamp_col='block_timestamp')
                #logger.warning('df loaded:%s',self.df.head())
            except Exception:
                logger.warning('load df',exc_info=True)

        def prep_data(self):
            try:
                #self.df = dd.dataframe.from_pandas(self.df,npartitions=10)
                # make timestamp into index
                logger.warning('%s',self.df['block_timestamp'].head())
                self.df1 = self.df.set_index('block_timestamp')
            except Exception:
                logger.warning('load df',exc_info=True)

        def plot_account_activity(self,launch=-1):
            try:
                df = self.df1
                if self.update_type != 'all':
                    df = df[df['update_type'] == self.update_type]
                if self.account_type != 'all':
                    df = df[df['account_type'] == self.account_type]

                logger.warning('df columns:%s',df.columns)

                df = df[df.amount >= 0]
                #logger.warning('line 100 df:%s',df.head(30))
                df = df.resample(self.period).agg({'address':'count'})
                df = df.reset_index()
                df = df.compute()
                df = df.rename(index=str,columns={'address':'period_activity'})

                df['activity_delta(%)'] = df['period_activity'].pct_change(fill_method='ffill')
                df['activity_delta(%)'] = df['activity_delta(%)'].multiply(100)
                df = df.fillna(0)

                logger.warning('df in balance after resample:%s',df.tail(10))

                # make timestamp into index
                return df.hvplot.line(x='block_timestamp', y=['period_activity'],
                                      title='# of transactions')+\
                       df.hvplot.line(x='block_timestamp', y=['activity_delta(%)'],
                                      title='% change in # of transactions')
                # make timestamp into index
            except Exception:
                logger.warning('plot account activity',exc_info=True)

        def plot_account_status(self, launch=-1):
            try:
                state = self.status
                #logger.warning('df1 head:%s',self.df1.columns)
                df = self.df1
                if self.account_type != 'all':
                    df = self.df1[self.df1['account_type'] == self.account_type]

                df = df[df['status'] == state]
                df = df.resample(self.period).agg({'status': 'count'})

                df = df.reset_index()
                df = df.compute()
                df['perc_change'] = df['status'].pct_change(fill_method='ffill')
                df.perc_change = df.perc_change.multiply(100)
                df = df.fillna(0)
                # df = self.clean_data(df)

                # make timestamp into index
                value_label = '# '+state
                gc.collect()
                title1 = 'accounts {} by period'.format(state)
                title2 = 'percentage {} change by period'.format(state)
                return df.hvplot.line(x='block_timestamp', y=['status'], value_label=value_label,
                                      title=title1) + \
                       df.hvplot.line(x='block_timestamp', y=['perc_change'], value_label='%',
                                      title=title2)
            except Exception:
                logger.error('plot account status', exc_info=True)



        def title_div(self, text, width=700):
            text = '<h2 style="color:#4221cc;">{}</h2>'.format(text)
            return Div(text=text, width=width, height=15)

        def corr_information_div(self, width=400, height=300):
            div_style = """ 
                           style='width:350px; margin-left:-500px;
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
                logger.warning(' df:%s',df.head(10))
                a = df[self.variable].tolist()
                for col in df.columns.tolist():
                    logger.warning('col :%s', col)

                    if col != self.variable:
                        logger.warning('%s:%s', col, self.variable)
                        b = df[col].tolist()
                        slope, intercept, rvalue, pvalue, std_err = linregress(a, b)
                        logger.warning('slope:%s,intercept:%s,rvalue:%s,pvalue:%s,std_err:%s',
                                     slope, intercept, rvalue, pvalue, std_err)
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
                logger.warning('df:%s',df.head(23))
                return df.hvplot.table(columns=['Variable 1', 'Variable 2','Relationship','r','p-value'],
                                       width=700,height=400,title='Correlation between variables')
            except Exception:
                logger.warning('correlation table', exc_info=True)


        def matrix_plot(self,launch=-1):
            try:
                #logger.warning('line 306 self.feature list:%s',self.feature_list)

                if self.update_type != 'all':
                    df = self.df1[self.df1['update_type'] == self.update_type]
                else:
                    df = self.df1
                #df = df[self.feature_list]

                # get difference for money columns
                #logger.warning('line 282 df; %s', list(df.columns))

                df = df.resample(self.period).mean()
                #logger.warning('line 285 df; %s', self.groupby_dict)

                df = df.reset_index()
                #logger.warning('line 286 df; %s', df.head())

                df = df.drop('block_timestamp',axis=1)
                df = df.fillna(0)
                df = df.compute()

                df['aion_close'] = df['aion_close']
                df['aion_market_cap'] = df['aion_market_cap']
                df['bitcoin_close'] = df['bitcoin_close']
                df['ethereum_close'] = df['ethereum_close']
                df['bitcoin_market_cap'] = df['aion_market_cap']
                df['ethereum_market_cap'] = df['aion_market_cap']

                df = df.fillna(0)
                #logger.warning('line 302. df: %s',df.head(10))

                self.corr_df = df.copy()
                cols_lst = df.columns.tolist()
                cols_temp = cols_lst.copy()
                if self.variable in cols_temp:
                    cols_temp.remove(self.variable)
                variable_select.options = cols_lst
                logger.warning('line 305 cols temp:%s',cols_temp)
                logger.warning('line 306 self.variable:%s',self.variable)
                logger.warning('line 307 df columns:%s',df.columns)

                p = df.hvplot.scatter(x=self.variable,y=cols_temp,width=400,
                                      subplots=True,shared_axes=False,xaxis=False).cols(3)

                return p

            except Exception:
                logger.error('matrix plot', exc_info=True)


    def update(attrname, old, new):
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.load_df(datepicker_start.value,datepicker_end.value)
        thistab.prep_data()
        thistab.update_type = update_type_select.value
        thistab.status = status_select.value
        thistab.account_type = account_type_select.value
        thistab.variable = variable_select.value
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        stream_launch_matrix.event(launch=thistab.trigger)
        thistab.notification_updater("Ready.")

    def update_resample(attr,old,new):
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.prep_data()
        thistab.period = new
        thistab.update_type = update_type_select.value
        thistab.status = status_select.value
        thistab.account_type = account_type_select.value
        thistab.variable = variable_select.value
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        stream_launch_matrix.event(launch=thistab.trigger)
        stream_launch_corr.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    def update_account_type(attr, old, new):
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.prep_data()
        thistab.account_type = new
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        stream_launch_matrix.event(launch=thistab.trigger)
        stream_launch_corr.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    def update_update_type(attr, old, new):
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.prep_data()
        thistab.update_type = new
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        stream_launch_matrix.event(launch=thistab.trigger)
        stream_launch_corr.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    def update_variable(attr, old, new):
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.prep_data()
        thistab.variable = new
        thistab.trigger += 1
        stream_launch_matrix.event(launch=thistab.trigger)
        stream_launch_corr.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    def update_status(attr, old, new):
        thistab.notification_updater("Calculations in progress! Please wait.")
        thistab.prep_data()
        thistab.status = new
        thistab.trigger += 1
        stream_launch.event(launch=thistab.trigger)
        stream_launch_matrix.event(launch=thistab.trigger)
        stream_launch_corr.event(launch=thistab.trigger)
        thistab.notification_updater("Ready!")

    try:
        
        cols = list(set(hyp_variables + ['address','update_type','account_type','balance',
                                         'status','block_timestamp','timestamp_of_first_event']))
        thistab = Thistab(table='account_ext_warehouse',cols=cols)
        # STATIC DATES
        # format dates
        first_date_range = "2018-04-25 00:00:00"
        first_date_range = datetime.strptime(first_date_range, thistab.DATEFORMAT)
        last_date_range = datetime.now().date()
        last_date = dashboard_config['dates']['last_date']
        first_date = datetime_to_date(last_date - timedelta(days=DAYS_TO_LOAD))
        '''
        thistab.df = thistab.pym.load_df(start_date=first_date, end_date=last_date,
                            cols=cols,table='account_ext_warehouse',timestamp_col='block_timestamp')
        '''
        thistab.load_df(start_date=first_date, end_date=last_date,cols=cols,
                                     timestamp_col='block_timestamp')
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
                               value=thistab.period,
                               options=menus['period'])

        variable_select = Select(title='Select variable',
                                 value='aion_fork',
                                 options=sorted(hyp_variables))
        status_select = Select(title='Select account status',
                               value=thistab.status,
                               options=menus['status'])
        account_type_select = Select(title='Select account type',
                                     value=thistab.account_type,
                                     options=menus['account_type'])
        update_type_select = Select(title='Select transfer type',
                                    value=thistab.update_type,
                                    options=menus['update_type'])

        # --------------------- PLOTS----------------------------------
        width = 800
        hv_account_status = hv.DynamicMap(thistab.plot_account_status,
                                           streams=[stream_launch]).opts(plot=dict(width=width, height=400))
        hv_account_activity = hv.DynamicMap(thistab.plot_account_activity,
                                            streams=[stream_launch]).opts(plot=dict(width=width, height=400))
        hv_matrix_plot = hv.DynamicMap(thistab.matrix_plot,
                                       streams=[stream_launch_matrix])
        hv_corr_table = hv.DynamicMap(thistab.correlation_table,
                                      streams=[stream_launch_corr])
        hv_hist_plot = hv.DynamicMap(thistab.hist,streams=[stream_launch_corr])

        account_status = renderer.get_plot(hv_account_status)
        account_activity = renderer.get_plot(hv_account_activity)
        matrix_plot = renderer.get_plot(hv_matrix_plot)
        corr_table = renderer.get_plot(hv_corr_table)
        hist_plot = renderer.get_plot(hv_hist_plot)

        # handle callbacks
        datepicker_start.on_change('value', update)
        datepicker_end.on_change('value', update)
        period_select.on_change('value',update_resample)
        update_type_select.on_change('value',update_update_type)
        account_type_select.on_change('value',update_account_type)
        variable_select.on_change('value',update_variable)
        status_select.on_change('value',update_status)


        # COMPOSE LAYOUT
        # put the controls in a single element
        controls = WidgetBox(
            datepicker_start,
            datepicker_end,
            period_select,
            update_type_select,
            account_type_select,
            status_select,
            variable_select)


        # create the dashboards
        grid = gridplot([
            [thistab.notification_div['top']],
            [Spacer(width=20, height=50)],
            [thistab.section_headers['relationships']],
            [Spacer(width=20, height=30)],
            [matrix_plot.state,controls],
            [corr_table.state, thistab.corr_information_div()],
            [hist_plot.state],
            [thistab.section_headers['account activity']],
            [Spacer(width=20, height=30)],
            [account_status.state],
            [account_activity.state],
            [thistab.notification_div['bottom']]
            ])

        # Make a tab with the layout
        tab = Panel(child=grid, title=thistab.panel_title)
        return tab

    except Exception:
        logger.error('rendering err:', exc_info=True)
        return tab_error_flag(thistab.panel_title)
