from datetime import datetime, timedelta, date
from os.path import dirname, join
from statistics import mean

import pydot
from bokeh.layouts import gridplot
from bokeh.models import Panel, Div, DatePicker, WidgetBox, Button, CustomJS, Paragraph, CheckboxGroup, Select
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
from scripts.utils.myutils import datetime_to_date
from scripts.streaming.streamingDataframe import StreamingDataframe as SD

from tornado.gen import coroutine
from config.df_construct_config import table_dict

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
    'block_size', 'block_time', 'difficulty', 'nrg_limit',
    'nrg_reward', 'num_transactions', 'block_nrg_consumed', 'nrg_price',
    'transaction_nrg_consumed', 'value']


def label_state(row):
    if "churned" == row['activity']:
        #logger.warning('INSIDE coded match:%s', row['activity'])
        return 1
    return 0


def code_variable(df):
    try:
        if df is not None:
            if len(df) > 0:
                df['churned'] = df.apply(label_state,axis=1,meta=object)
                return df['churned']
    except Exception:
        logger.error("label churned retained:", exc_info=True)


@coroutine
def account_predictive_tab():
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
            self.feature_list = hyp_variables
            self.targets = ['churned']
            self.interest_var = 'address'
            self.trigger = -1

            self.clf = None
            self.pl = {}  # for rf pipeline
            self.div_style = """ style='width:300px; margin-left:25px;
            border:1px solid #ddd;border-radius:3px;background:#efefef50;' 
            """
            self.header_style = """ style='color:blue;text-align:center;' """
            txt = """<div style="text-align:center;background:black;width:100%;">
                     <h1 style="color:#fff;">
                     {}</h1></div>""".format('Welcome')
            self.notification_div = Div(text=txt, width=1400, height=20)
            self.notification_div_bottom = Div(text=txt, width=1400, height=20)
            # list of tier specific addresses for prediction
            self.address_list = []
            self.prediction_address_selected = ""
            self.load_data_flag = False
            self.day_diff = 1
            self.groupby_dict = {
                'value': 'mean',
                'block_nrg_consumed': 'mean',
                'transaction_nrg_consumed': 'mean',
                'difficulty': 'mean',
                'nrg_limit': 'mean',
                'block_size': 'mean',
                'block_time': 'mean',
                'nrg_reward': 'mean',
                'num_transactions': 'mean',
                'nrg_price':'mean'
            }
            self.div_style = """ style='width:300px; margin-left:25px;
                        border:1px solid #ddd;border-radius:3px;background:#efefef50;' 
                        """
            self.metrics_div = Div(text='',width=400,height=300)
            self.accuracy_df = None
            self.inspected_variable = 'value'

        def notification_updater(self, new_text):
            txt = """<div style="text-align:center;background:black;width:100%;">
                     <h4 style="color:#fff;">
                     {}</h4></div>""".format(new_text)
            self.notification_div.text = txt
            self.notification_div_bottom.text = txt

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
                self.prediction_address_selected = ""
                self.prediction_address_select.value = "all"
            except Exception:
                logger.error('reset checkboxes', exc_info=True)

        ###################################################
        #               I/O
        def load_df(self, start_date="2018-04-23 00:00:00", end_date="2018-12-10 00:00:00"):
            try:
                if isinstance(start_date, str):
                    start_date = datetime.strptime(start_date, self.DATEFORMAT)
                if isinstance(end_date, str):
                    end_date = datetime.strptime(end_date, self.DATEFORMAT)
                self.df_load(start_date, end_date)

                    #self.make_delta()
                    #self.df = self.df.set_index('block_timestamp')
                # logger.warning("data loaded - %s",self.tab.df.tail(10))

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


        def split_df(self, df,cols=['churned','active']):
            for col in cols:
                self.df1[col] = df['activity'] == col
            logger.warning("Finished split into churned and retained dataframes")

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
                    df = SD('filter', [variable, 'activity'], []).get_df()

                return df.hvplot.box(variable, by='activity',
                                                  ylim=(.9 * minv, 1.1 * maxv))
            except Exception:
                logger.error("box plot:", exc_info=True)

        ###################################################
        #               MODELS
        def rf_clf(self):
            try:
                logger.warning("RANDOM FOREST LAUNCHED")

                error_lst = []
                df = self.df
                df = self.normalize(df)
                for target in self.targets:
                    # if all addresses used filter for only positive transactions
                    # else double entry will render as sums as
                    df['churned'] = df.apply(label_state,axis=1,meta=('x','i8'))

                    #logger.warning("line 205: df columns in %s:",df.columns.tolist())
                    df = df.groupby(['address','churned']).agg(self.groupby_dict)
                    #logger.warning("line 205: df columns in %s:",df.tail(10))
                    df = df.reset_index()

                    df1 = df[df.churned == 1]
                    logger.warning('churned column count after grouping:%s', len(df1))

                    df = df.compute()
                    X = df[self.feature_list]
                    y = df[target]

                    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3)
                    self.feature_list = X_train.columns.tolist()

                    self.pl['churned'] = Pipeline([
                        ('imp', SimpleImputer(missing_values=0,
                                              strategy='median')),
                        ('rf', RandomForestClassifier(n_estimators=100, random_state=42,
                                                      max_depth=4,
                                                      class_weight='balanced'))
                    ])
                    self.pl['churned'].fit(X_train, y_train)

                    y_pred = self.pl['churned'].predict(X_test)
                    error_lst.append(round(100*metrics.accuracy_score(y_test, y_pred),2))

                self.accuracy_df = pd.DataFrame(
                    {
                        'Variable': self.targets,
                        'Accuracy': error_lst,
                     })
                logger.warning('accuracy_df:%s',self.accuracy_df.head())
                self.make_tree(target='churned')

                print('confusion matrix:\n')
                print(confusion_matrix(y_test, y_pred))
                print('classification report:\n')
                print(classification_report(y_test, y_pred))
                #logger.warning("clf model built:%s",self.pl)

            except Exception:
                logger.error("RF:", exc_info=True)

        def accuracy_table(self):
            try:
                columns = self.accuracy_df.columns.tolist()
                return self.accuracy_df.hvplot.table(columns=['Variable','Accuracy'], width=250,
                                       title='Prediction accuracy')

            except Exception:
                logger.error("RF:", exc_info=True)

        def prediction_information_div(self, width=350, height=450):
            txt = """
            <div {}>
            <h4 {}>Info </h4>
            <ul style='margin-top:-10px;'>
            <li>
            The table shows the predicted change.</br>
            </li>
            <li>
            For desirable outcomes:
            </br> ... a positive number is good!
            </br> ... the bigger the number the better.
            </br> ... a negative number is bad!
            </br> ... the bigger the negative number the worse it is.
            </li>
            <>
            For non-desirable outcomes:
            </br>... the inverse is true
            </li>
            <li>
            Use the datepicker(s) to select dates for the period desired
            </li>
            </ul>
            </div>

            """.format(self.div_style, self.header_style)
            div = Div(text=txt, width=width, height=height)
            return div

        def stats_information_div(self, width=400, height=300):
            txt = """
            <div {}>
                   <h4 {}>Metadata Info </h4>
                   <ul>
                   <li >
                   <h4 style='margin-bottom:-2px;'>Table left:</h4>
                   - shows the outcome,</br>
                     and the accuracy in %</br>
                     <strong><i>100% is perfection!</i></strong>
                   </li>
                   <li>
                   <h4 style='margin-bottom:-2px;'>Table right:</h4>
                     - shows the desired outcome, the variables(things Aion controls)
                   </br> and their importance to the particular outcome
                   </br> ...which variable(s) have a greater impact on an outcome.
                   </br>- lower = better
                   </br>- generally only the best ranked 3 matter
                   </br>- business advice: manipulate the top ranked variables to attain desirable outcomes
                   </li>
                   </ul>
            </div>""".format(self.div_style, self.header_style)
            div = Div(text=txt, width=width, height=height)
            return div

        def load_prediction_df(self,start_date,end_date):
            if isinstance(start_date, date):
                start_date = datetime.combine(start_date, datetime.min.time())
            if isinstance(end_date, date):
                end_date = datetime.combine(end_date, datetime.min.time())
            self.df_predict = self.cl.load_data(table='account_activity_warehouse', cols=self.feature_list +
                                                                            ['address', 'block_timestamp'],
                                                start_date=start_date, end_date=end_date)
            logger.warning('319:in load prediction: %s',self.df_predict.head(5))


        def update_prediction_addresses_select(self):
            self.prediction_address_select.options = ['all']
            if len(self.df_predict) > 0:
                lst = ['all'] + list(self.df_predict['address'].unique().compute())
                self.prediction_address_select.options = lst


        # the period for which the user wants a prediction
        def make_account_predictions(self,launch=-1):
            try:
                logger.warning("MAKE PREDICTIONS LAUNCHED")
                target = self.targets[0]
                # make
                df = self.df_predict
                #logger.warning("line 311%s",df.head(10))
                # make list of address for prediction select
                # filter if prediction for certain addresses
                logger.warning('address selected:%s',self.prediction_address_select.value)
                if self.prediction_address_select.value is not None:
                    if len(self.prediction_address_select.value) > 0:
                        if self.prediction_address_select.value not in ['all', '']:
                            df = df[df.address == self.prediction_address_select.value]

                #logger.warning('line 409 predict-df post filter:%s', df.head(20))
                if len(df) > 0:
                    df = self.normalize(df)
                    df = self.group_data(df,self.groupby_dict)
                    interest_labels = list(df['address'].unique())

                    # run model
                    df = df.fillna(0)
                    X = df[self.feature_list]
                    # logger.warning("df before prediction:%s",X.tail(10))
                    y_pred = self.pl['churned'].predict(X)
                    y_pred_verbose = ['to leave' if x in ["1", 1] else "to remain" for x in y_pred]
                    # make table for display
                    self.predict_df = pd.DataFrame({
                        'address': interest_labels,
                        'likely...': y_pred_verbose
                    })
                    perc_to_churn = round(100 * sum(y_pred) / len(y_pred), 1)
                    if 'churn' in target:
                        txt = 'churn'
                    else:
                        txt = 'remain'
                    text = """<div {}>
                    <h3>Percentage likely to {}:</h3>
                    <strong 'style=color:black;'>{}%</strong></div>""".format(self.header_style,
                                                                              txt,
                                                                              perc_to_churn)
                    self.metrics_div.text = text
                else:
                    # make table for display
                    self.predict_df = pd.DataFrame({
                        'address': [],
                        'likely...': []
                    })
                    text = """<div {}>
                        <br/> <h3>Sorry, address not found</h3>
                        </div>""".format(self.header_style)
                    self.metrics_div.text = text
                logger.warning("end of predictions")
                return self.predict_df.hvplot.table(columns=['address', 'likely...'], width=500,
                                       title='Account Churn predictions')
            except Exception:
                logger.error("prediction:", exc_info=True)

        def make_tree(self, target='churned'):
            try:
                if not self.pl:
                    self.rf_clf()
                # Limit depth of tree to 3 levels
                # Extract the small tree
                tree_small = self.pl[target].named_steps['rf'].estimators_[5]
                # Save the tree as a png image
                export_graphviz(tree_small, out_file='small_tree.dot',
                                feature_names=self.feature_list, rounded=True,
                                precision=1)

                (graph,) = pydot.graph_from_dot_file('small_tree.dot')
                # filepath = self.make_filepath('../../../static/images/small_tree.gif')
                # .write_png(filepath)
                filepath = self.make_filepath('/home/andre/Downloads/small_tree.png')
                graph.write_png(filepath)
                logger.warning("TREE SAVED")
            except Exception:
                logger.error("make tree:", exc_info=True)

        def make_feature_importances(self):
            try:
                if not self.pl:
                    self.rf_clf()

                results_dct = {
                    'outcome': [],
                    'feature': [],
                    'importance': [],
                    'rank_within_outcome': []
                }
                for target in self.targets:
                    logger.warning('make feature importances for :%s', target)
                    # Get numerical feature importances
                    importances = list(self.pl[target].named_steps['rf'].feature_importances_)

                    # List of tuples with variable and importance
                    feature_importances = [(feature, round(importance, 4)) for feature, importance in
                                           zip(self.feature_list, importances)]

                    sorted_importances = sorted(feature_importances, key=itemgetter(1))

                    # logger.warning('importances :%s',importances)
                    # logger.warning("feature_importances:%s",feature_importances)
                    target_lst = [target] * len(importances)

                    count = 1
                    rank_lst = []
                    for i in importances:
                        rank_lst.append(count)
                        count += 1

                    results_dct['outcome'] += target_lst
                    results_dct['feature'] += [i[0] for i in sorted_importances]
                    results_dct['importance'] += [i[1] for i in sorted_importances]
                    results_dct['rank_within_outcome'] += sorted(rank_lst,reverse=True)

                df = pd.DataFrame.from_dict(results_dct)
                logger.warning('MAKE FEATURE IMPORTANCES FINISHED')
                return df.hvplot.table(columns=['outcome', 'feature', 'importance', 'rank_within_outcome'],
                                       width=600,
                                       title="Variables ranked by importance (for each output)")

            except Exception:
                logger.error("Feature importances:", exc_info=True)

        ####################################################
        #               GRAPHS
    def update(attrname, old, new):
        this_tab.notification_updater("Calculations underway. Please be patient")
        this_tab.load_prediction_df(datepicker_start.value, datepicker_end.value)
        this_tab.update_prediction_addresses_select()
        this_tab.trigger += 1
        stream_launch.event(launch=this_tab.trigger)
        stream_select_variable.event(variable=this_tab.inspected_variable)
        this_tab.notification_updater("ready")

    def update_address_predictions(attrname, old, new):
        this_tab.notification_updater("Calculations underway. Please be patient")
        this_tab.trigger += 1
        stream_launch.event(launch=this_tab.trigger)
        this_tab.notification_updater("ready")

    def update_select_variable(attrname, old, new):
        this_tab.notification_updater("Calculations underway. Please be patient")
        this_tab.inspected_variable = select_variable.value
        stream_select_variable.event(variable=this_tab.inspected_variable)
        this_tab.notification_updater("ready")

    try:
        # SETUP
        table = 'account_activity_warehouse'
        #cols = list(table_dict[table].keys())
        cols = ['address','day_of_week', 'block_size', 'block_timestamp',
                'block_time', 'difficulty', 'nrg_limit',
                'nrg_reward', 'num_transactions', 'block_nrg_consumed', 'nrg_price',
                'transaction_nrg_consumed', 'value','event','activity','account_type']
        this_tab = Thistab(table, cols, [])
        this_tab.load_df()
        this_tab.rf_clf()

        # setup dates
        first_date_range = datetime.strptime("2018-04-23 00:00:00", "%Y-%m-%d %H:%M:%S")
        last_date_range = datetime.now().date()
        range = 5
        last_date = datetime.strptime("2018-06-05 00:00:00", "%Y-%m-%d %H:%M:%S")
        first_date = datetime_to_date(last_date - timedelta(days=range))

        # STREAMS Setup
        # date comes out stream in milliseconds
        stream_launch = streams.Stream.define('Launch',launch=-1)()
        stream_select_variable = streams.Stream.define('Select_variable',
                                                       variable='value')()

        # setup widgets
        datepicker_start = DatePicker(title="Start", min_date=first_date_range,
                                      max_date=last_date_range, value=first_date)
        datepicker_end = DatePicker(title="End", min_date=first_date_range,
                                    max_date=last_date_range, value=last_date)
        select_variable = Select(title='Filter by address',value=this_tab.inspected_variable,
                                 options=this_tab.feature_list)

        # search by address checkboxes
        this_tab.prediction_address_select = Select(
            title='Filter by address',
            value='all',
            options=[])
        reset_prediction_address_button = Button(label="reset address(es)", button_type="success")

        # load predict data
        this_tab.load_prediction_df(datepicker_start.value, datepicker_end.value)
        this_tab.update_prediction_addresses_select()

        # tables
        hv_account_prediction_table = hv.DynamicMap(this_tab.make_account_predictions,
                                                    streams=[stream_launch])
        account_prediction_table = renderer.get_plot(hv_account_prediction_table)

        hv_features_table = hv.DynamicMap(this_tab.make_feature_importances)
        features_table = renderer.get_plot(hv_features_table)

        hv_accuracy_table = hv.DynamicMap(this_tab.accuracy_table)
        accuracy_table = renderer.get_plot(hv_accuracy_table)


        hv_variable_plot = hv.DynamicMap(this_tab.box_plot,
                                 streams=[stream_select_variable])\
            .opts(plot=dict(width=800, height=500))

        variable_plot = renderer.get_plot(hv_variable_plot)


        # add callbacks
        datepicker_start.on_change('value', update)
        datepicker_end.on_change('value', update)
        this_tab.prediction_address_select.on_change('value', update_address_predictions)
        reset_prediction_address_button.on_click(this_tab.reset_checkboxes)
        select_variable.on_change('value',update_select_variable)


        # put the controls in a single element
        date_controls = WidgetBox(datepicker_start, datepicker_end,
                                  this_tab.prediction_address_select,
                                  reset_prediction_address_button)

        grid = gridplot([
            [this_tab.notification_div],
            [this_tab.title_div('Predictions for churned accounts ', 600)],
            [accuracy_table.state,this_tab.stats_information_div(), features_table.state],
            [this_tab.title_div('Variable behaviour: ', 600)],
            [select_variable, variable_plot.state],
            [this_tab.title_div('Select period below to obtain predictions:', 600)],
            [date_controls, account_prediction_table.state,this_tab.metrics_div],
            [this_tab.notification_div_bottom]
        ])

        tab = Panel(child=grid, title='Account prediction')
        return tab

    except Exception:
        logger.error('rendering err:', exc_info=True)
        text = 'account predictions'
        return tab_error_flag(text)
