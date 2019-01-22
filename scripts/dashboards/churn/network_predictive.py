from datetime import datetime
from os.path import dirname, join
from statistics import mean

import pydot
from bokeh.layouts import gridplot
from bokeh.models import Panel, Div, DatePicker, WidgetBox
from bokeh.plotting import figure
from scipy import stats
from sklearn import metrics
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.metrics import confusion_matrix, classification_report
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.tree import export_graphviz
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline

from scripts.storage.pythonClickhouse import PythonClickhouse
from scripts.utils.dashboards.mytab import Mytab
from scripts.utils.mylogger import mylogger
from scripts.utils.modeling.churn.miner_predictive_tab import MinerChurnedPredictiveTab

from tornado.gen import coroutine
from config.df_construct_config import load_columns as columns

from operator import itemgetter
import pandas as pd
import dask as dd
import numpy as np
import holoviews as hv
import hvplot.pandas
import hvplot.dask
from holoviews import opts, streams
import datashader as ds
from holoviews.operation.datashader import datashade, shade, dynspread, rasterize
from holoviews.operation import decimate
from holoviews.operation.timeseries import rolling, rolling_outlier_std


from scripts.utils.myutils import tab_error_flag

logger = mylogger(__file__)

hv.extension('bokeh', logo=False)
renderer = hv.renderer('bokeh')

hyp_variables = [
                'block_size', 'block_time','difficulty', 'nrg_limit',
                'approx_nrg_reward' , 'num_transactions','block_nrg_consumed','nrg_price',
                'approx_value', 'transaction_nrg_consumed']

@coroutine
def network_churn_predictive_tab():
    class Thistab(Mytab):
        def __init__(self,table,cols,dedup_cols):
            Mytab.__init__(self,table,cols,dedup_cols)
            self.table = 'miner_activity'
            self.cols = cols[self.table]
            self.DATEFORMAT = "%Y-%m-%d"
            self.df = None
            self.rf = {} # random forest
            self.cl = PythonClickhouse('aion')
            self.feature_list = hyp_variables
            self.targets = ['tier1_retained_diff','tier2_retained_diff',
                           'tier1_churned_diff','tier2_churned_diff']
            self.pl = {}

        ###################################################
        #               I/O
        def load_df(self,start_date="2018-04-23",end_date=datetime.now().date()):
            try:
                if isinstance(start_date,str):
                    start_date = datetime.strptime(start_date,self.DATEFORMAT).date()
                if isinstance(end_date, str):
                    end_date = datetime.strptime(end_date, self.DATEFORMAT)
                self.df_load(start_date,end_date)
                self.make_delta()
                #logger.warning("data loaded - %s",self.tab.df.tail(10))
                self.df = self.df.set_index('block_timestamp')
                #logger.warning("data loaded - %s",self.tab.df.tail(10))

            except Exception:
                logger.error('load_df', exc_info=True)

        def make_filepath(self, path):
            return join(dirname(__file__), path)

        ###################################################
        #               MUNGE DATA
        def make_delta(self):
            try:
                if len(self.df) > 0:
                    df = self.df.compute()
                    for col in ['tier1_new','tier1_churned','tier1_retained',
                                'tier2_new', 'tier2_churned', 'tier2_retained']:
                        col_new = col +'_diff'
                        df[col_new] = df[col].pct_change()
                        df[col_new] = df[col_new].fillna(0)
                        logger.warning('diff col added : %s',col_new)
                    self.df = self.df.fillna(self.df.mean())
                    self.df = dd.dataframe.from_pandas(df, npartitions=15)
                    #logger.warning('POST DELTA:%s',self.df1.tail(20))

            except Exception:
                logger.error('load_df', exc_info=True)

        ###################################################
        #               MODELS

        def rf_table(self):
            try:
                self.notification_updater("RF calculations underway")
                error_lst = []

                for target in self.targets:
                    df = self.df.compute()
                    df[df == np.inf] = np.nan
                    df = df.reset_index()
                    logger.warning('RF working on %s',target)

                    df.fillna(df.mean(),inplace=True)
                    y = df[target]
                    X = df[self.feature_list]
                    logger.warning('feature matrix:%s', X.columns.tolist())

                    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3)

                    self.pl[target] = Pipeline([
                        ('imp', SimpleImputer(missing_values=0,
                                              strategy='median')),
                        ('rf', RandomForestRegressor(n_estimators=100, random_state=42, max_depth=4,
                                                     max_features=None))
                    ])
                    self.pl[target].fit(X_train, y_train)

                    y_pred = self.pl[target].predict(X_test)
                    #error_dct[target] =cross_val_score(self.rf[target],X,y,cv=3)
                    #error_lst.append(self.pl[target].score(X_test, y_test))
                    error_lst.append(metrics.mean_absolute_error(y_test, y_pred))
                df = pd.DataFrame(
                    {'variable': self.targets,
                     'Mean (abs) error': error_lst,
                     })
                logger.warning('%s',df.head(1))
                return df.hvplot.table(columns=['variable','Mean (abs) error'],width=500,)
            except Exception:
                logger.error("RF:", exc_info=True)

        def information_div(self,width=400,height=300,text=''):
            txt = """
            <h4 style='color:green;padding-left:30px'>Info </h4>
            <ul>
            <li>
            The table shows the % predicted change.</br>
            For desirable outcomes:
            </br> ...a positive number is good!
            </br> ... the bigger the number the better.
            </br> ... a negative number is bad!
            </br> ... the bigger the negative number the worse it is.
            </li>
            <li>
            Use the datepicker(s) to select dates for the period desired
            </li>
            </ul>
            
            """
            div = Div(text=txt,width=width,height=height)
            return div

        def make_prediction(self,start_date,end_date):
            try:
                if not self.pl:
                    self.rf_table()

                #read from data warehouse
                if isinstance(start_date,datetime):
                    start_date = start_date.date()
                if isinstance(end_date, datetime):
                    end_date = end_date.date()

                df = self.cl.load_data('block_tx_warehouse',self.feature_list,start_date,end_date)
                logger.warning('%s',df['block_time'].mean().compute())

                # make summaries of the data
                block_size, block_time, difficulty, nrg_limit, approx_nrg_reward, num_transactions, \
                block_nrg_consumed, transaction_nrg_consumed, nrg_price, approx_value = \
                    dd.compute(df.block_size.mean(), df.block_time.mean(), df.difficulty.mean(),
                               df.nrg_limit.mean(), df.approx_nrg_reward.mean(), df.num_transactions.mean(),
                               df.block_nrg_consumed.mean(), df.transaction_nrg_consumed.mean(),
                               df.nrg_price.mean(),df.approx_value.mean())

                X = [[round(block_size), round(block_time), round(difficulty), round(nrg_limit),
                     round(approx_nrg_reward), round(num_transactions),
                     round(block_nrg_consumed), round(transaction_nrg_consumed),
                     round(nrg_price), round(approx_value)]]

                predictions_lst = []
                for target in self.targets:

                    predictions_lst.append(self.pl[target].predict(X))
                    logger.warning('MAKE PREDICTIONS COMPLETED FOR :%s', target)
                df = pd.DataFrame(
                    {'Outcome': self.targets,
                     'percentage_change': predictions_lst,
                     })
                return df.hvplot.table(columns=['Outcome','percentage_change'],width=500)
            except Exception:
                logger.error("MAKE PREDICTIONS:", exc_info=True)



        def make_tree(self,target='tier1_churned_diff'):
            try:
                if not self.pl:
                    self.rf_table()
                # Limit depth of tree to 3 levels
                # Extract the small tree
                tree_small = self.pl[target].named_steps['rf'].estimators_[5]
                # Save the tree as a png image
                export_graphviz(tree_small, out_file='small_tree.dot',
                                feature_names=self.feature_list[0:-1], rounded=True,
                                precision=1)

                (graph,) = pydot.graph_from_dot_file('small_tree.dot')
                filepath = self.make_filepath('../../../static/images/small_tree.png')
                graph.write_png(filepath)

                # main.py file
                x_range = (-20, -10)  # could be anything - e.g.(0,1)
                y_range = (20, 30)
                p = figure(x_range=x_range, y_range=y_range)
                # img_path = 'https://bokeh.pydata.org/en/latest/_static/images/logo.png'
                img_path = 'aion-analytics/static/images/small_tree.png'
                p.image_url(url=[img_path], x=x_range[0], y=y_range[1], w=x_range[1] - x_range[0],
                            h=y_range[1] - y_range[0])
                return p

            except Exception:
                logger.error("make tree:", exc_info=True)

        def make_feature_importances(self):
            try:
                if not self.pl:
                    self.rf_table()

                results_dct = {
                    'outcome':[],
                    'feature' : [],
                    'importance':[],
                    'rank within IV':[]
                }
                for target in self.targets:
                    logger.warning('make feature importances for :%s',target)
                    # Get numerical feature importances
                    importances = list(self.pl[target].named_steps['rf'].feature_importances_)

                    # List of tuples with variable and importance
                    feature_importances = [(feature, round(importance, 4)) for feature, importance in
                                           zip(self.feature_list[0:-1], importances)]

                    sorted_importances = sorted(feature_importances, key=itemgetter(1))
                    logger.warning('not_sorted:%s',feature_importances)
                    logger.warning('sorted:%s',sorted_importances)

                    target_lst = [target for x in range(0,len(importances))]
                    logger.warning('importances :%s',importances)

                    results_dct['outcome'] += target_lst
                    results_dct['feature'] += [i[0] for i in sorted_importances]
                    results_dct['importance'] += [i[1] for i in sorted_importances]
                    results_dct['rank within IV'] += [str(i) for i in range(1,len(sorted_importances)+1)]

                df = pd.DataFrame.from_dict(results_dct)
                logger.warning('MAKE FEATURE IMPORTANCES FINISHED')
                return df.hvplot.table(columns=['outcome','feature','importance', 'rank within IV'],width=600)

            except Exception:
                logger.error("Feature importances:", exc_info=True)

        ####################################################
        #               GRAPHS
        def history_line_graphs(self,cols):
            try:
                df = self.df.compute()
                df = df[cols]
                return df.hvplot.line()
            except Exception:
                logger.error('history line graphs', exc_info=True)

        def history_bar_graphs(self, cols):
            try:
                df = self.df.compute()
                df = df[cols]
                return df.hvplot.bar()
            except Exception:
                logger.error('history line graphs', exc_info=True)

        def dow(self,cols,variable):
            try:
                df = self.df.compute()
                df = df[cols]
                maxval = max(df[variable])*1.2
                return df.hvplot.box(variable, by='day_of_week',ylim=(0,300))
            except Exception:
                logger.error('dow', exc_info=True)

    def update(attrname, old, new):
        this_tab.notification_updater_2("Calculations underway. Please be patient")
        stream_start_date.event(start_date=datepicker_start.value)
        stream_end_date.event(end_date=datepicker_end.value)
        this_tab.notification_updater_2("")


    try:
        # SETUP
        this_tab = Thistab('miner_activity',columns,[])
        this_tab.load_df()
        cols1 = ['tier1_new','tier1_churned']
        cols2 = ['tier2_new', 'tier2_churned']
        cols3 = ['tier1_new', 'tier1_churned','day_of_week']
        cols4 = ['tier2_new', 'tier2_churned', 'day_of_week']

        cols1_diff = ['tier1_retained_diff', 'tier1_churned_diff']
        cols2_diff = ['tier2_retained_diff', 'tier2_churned_diff']

        # setup dates
        first_date_range = datetime.strptime("2018-04-23 00:00:00", "%Y-%m-%d %H:%M:%S")
        last_date_range = datetime.now().date()

        first_date = datetime.strptime("2018-10-01 00:00:00", '%Y-%m-%d %H:%M:%S')
        last_date = last_date_range
        last_date = datetime.strptime("2018-12-30 00:00:00", '%Y-%m-%d %H:%M:%S')


        # STREAMS Setup
        # date comes out stream in milliseconds
        stream_start_date = streams.Stream.define('Start_date',
                                                  start_date=first_date)()
        stream_end_date = streams.Stream.define('End_date',
                                                end_date=last_date)()

        # setup widgets
        datepicker_start = DatePicker(title="Start", min_date=first_date_range,
                                      max_date=last_date_range, value=first_date)
        datepicker_end = DatePicker(title="End", min_date=first_date_range,
                                    max_date=last_date_range, value=last_date)

        #  MAKE GRAPHS
        # simple history
        hv_history_curves1 = hv.DynamicMap(this_tab.history_line_graphs(cols1)).\
            opts(plot=dict(width=600, height=400))
        history_curves1 = renderer.get_plot(hv_history_curves1)

        hv_history_curves2 = hv.DynamicMap(this_tab.history_line_graphs(cols2)).\
            opts(plot=dict(width=600, height=400))
        history_curves2 = renderer.get_plot(hv_history_curves2)

        # percentage difference history
        hv_history_curves1_diff = hv.DynamicMap(this_tab.history_line_graphs(cols1_diff)). \
            opts(plot=dict(width=600, height=400))
        history_curves1_diff = renderer.get_plot(hv_history_curves1_diff)

        hv_history_curves2_diff = hv.DynamicMap(this_tab.history_line_graphs(cols2_diff)). \
            opts(plot=dict(width=600, height=400))
        history_curves2_diff = renderer.get_plot(hv_history_curves2_diff)


        # tables
        hv_accuracy_table = hv.DynamicMap(this_tab.rf_table)
        accuracy_table = renderer.get_plot(hv_accuracy_table)

        hv_prediction_table = hv.DynamicMap(this_tab.make_prediction,
                                            streams=[stream_start_date,stream_end_date])
        prediction_table = renderer.get_plot(hv_prediction_table)

        hv_features_table = hv.DynamicMap(this_tab.make_feature_importances)
        features_table = renderer.get_plot(hv_features_table)

        # split by dow
        hv_dow1= hv.DynamicMap(this_tab.dow(cols3,'tier1_churned'))
        dow1 = renderer.get_plot(hv_dow1)

        hv_dow2 = hv.DynamicMap(this_tab.dow(cols4,'tier2_churned'))
        dow2 = renderer.get_plot(hv_dow2)

        hv_dow3 = hv.DynamicMap(this_tab.dow(cols3,'tier1_new'))
        dow3 = renderer.get_plot(hv_dow3)

        hv_dow4 = hv.DynamicMap(this_tab.dow(cols4,'tier2_new'))
        dow4 = renderer.get_plot(hv_dow4)

        # add callbacks
        datepicker_start.on_change('value', update)
        datepicker_end.on_change('value', update)

        # image
        p = this_tab.make_tree()

        # put the controls in a single element
        date_controls = WidgetBox(datepicker_start, datepicker_end)

        grid = gridplot([
            [this_tab.notification_div],
            [this_tab.title_div('Churned and new miners by date')],
            [history_curves1.state, history_curves2.state],
            [this_tab.title_div('Rolling % daily difference for churned and new miners')],
            [history_curves1_diff.state, history_curves2_diff.state],
            [this_tab.title_div('Distribution of churned and new miners by day of week')],
            [dow1.state,dow2.state,dow3.state,dow4.state],
            [this_tab.title_div('Prediction stats for new,churned,miners models ',600)],
            [accuracy_table.state,features_table.state],
            [p],
            [this_tab.title_div('Select period below to obtain predictions:', 600)],
            [date_controls,this_tab.information_div(),prediction_table.state]
        ])

        tab = Panel(child=grid, title='Network churn predictions')
        return tab

    except Exception:
        logger.error('rendering err:', exc_info=True)
        text = 'network churn predictions'
        return tab_error_flag(text)
