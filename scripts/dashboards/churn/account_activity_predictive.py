from datetime import datetime, timedelta
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
                'block_size', 'block_time','difficulty', 'nrg_limit',
                'nrg_reward' , 'num_transactions','block_nrg_consumed','nrg_price',
                'transaction_nrg_consumed','value']

@coroutine
def account_activity_predictive_tab():
    class Thistab(Mytab):
        def __init__(self,table,cols,dedup_cols):
            Mytab.__init__(self, table, cols, dedup_cols)
            self.table = table
            self.cols = cols
            self.DATEFORMAT = "%Y-%m-%d"
            self.df = None
            self.df1 = {} # to contain churned and retained splits
            self.day_diff = 1 # for normalizing for classification periods of different lengths
            self.df_grouped = ''

            self.rf = {}  # random forest
            self.cl = PythonClickhouse('aion')
            self.feature_list = hyp_variables
            self.targets = ['retained','new', 'churned']

            self.clf = None
            self.pl = {} # for rf pipeline
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

        def notification_updater(self, text):
            txt = """<div style="text-align:center;background:black;width:100%;">
                     <h4 style="color:#fff;">
                     {}</h4></div>""".format(text)
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

        def results_div(self, text, width=600, height=300):
            div = Div(text=text, width=width, height=height)
            return div

        def reset_checkboxes(self):
            try:
                self.prediction_address_selected = ""
                self.prediction_address_select.value = "all"
            except Exception:
                logger.error('reset checkboxes', exc_info=True)

        ###################################################
        #               I/O
        def load_df(self,start_date="2018-04-23",end_date="2018-12-10"):
            try:
                if isinstance(start_date,str):
                    start_date = datetime.strptime(start_date,self.DATEFORMAT).date()
                if isinstance(end_date, str):
                    end_date = datetime.strptime(end_date, self.DATEFORMAT).date()
                self.df_load(start_date,end_date)
                # make list of address for prediction select
                self.address_list = ['all'] + self.df['address'].unique().tolist()
                logger.warning("line 125: data loaded - %s",self.df.tail(10))
                self.make_delta()
                self.df = self.df.set_index('block_timestamp')
                #logger.warning("data loaded - %s",self.tab.df.tail(10))

            except Exception:
                logger.error('load_df', exc_info=True)



        ###################################################
        #               MUNGE DATA
        def make_delta(self):
            try:
                if self.df is not None:
                    if len(self.df) > 0:
                        df = self.df.compute()
                        for col in ['new','churned','retained',
                                    'new', 'churned', 'retained']:
                            col_new = col +'_diff'
                            df[col_new] = df[col].pct_change()
                            df[col_new] = df[col_new].fillna(0)
                            logger.warning('diff col added : %s',col_new)
                        self.df = self.df.fillna(self.df.mean())
                        self.df = dd.dataframe.from_pandas(df, npartitions=15)
                        #logger.warning('POST DELTA:%s',self.df1.tail(20))

            except Exception:
                logger.error('load_df', exc_info=True)


        """  daily normalization by dividing each column by number of 
          days spanned by a dataframe """


        def label_state(self, x):
            if x in self.churned_list:
                return 1
            return 0

        def label_state_verbose(self, x):
            if x in self.churned_list:
                return 'churned'
            return 'retained'

        def label_churned_retained(self, df):
            try:
                if df is not None:
                    if len(df) > 0:
                        df['churned'] = df[self.interest_var] \
                            .map(self.label_state)
                        logger.warning("Finished churned retained")
                        return df
            except Exception:
                logger.error("label churned retained:", exc_info=True)

        def label_churned_verbose(self, df):
            try:
                if df is not None:
                    if len(df) > 0:
                        df['churned_verbose'] = df[self.interest_var] \
                            .map(self.label_state_verbose)
                        logger.warning("Finished churned retained")
                        return df
            except Exception:
                logger.error("label churned retained:", exc_info=True)

        def split_df(self, df):
            self.df1['churned'] = df[df.churned == 1]
            self.df1['retained'] = df[df.churned == 0]
            logger.warning("Finished split into churned and retained dataframes")

        ###################################################
        #               MODELS

        def rf_table(self):
            try:
                self.notification_updater("RF calculations underway")
                error_lst = []

                for target in self.targets:
                    logger.warning("df before rf: %s",self.df.columns.tolist())
                    df = self.df.compute()
                    df[df == np.inf] = np.nan
                    df = df.reset_index()
                    logger.warning('RF working on %s',target)

                    df.fillna(df.mean(),inplace=True)
                    y = df[target]
                    X = df[self.feature_list]
                    #logger.warning('feature matrix:%s', X.columns.tolist())


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
                self.notification_updater("ready")

                return df.hvplot.table(columns=['variable','Mean (abs) error'],width=250,
                                       title='Difference between prediction and truth')
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
            </br> ...a positive number is good!
            </br> ... the bigger the number the better.
            </br> ... a negative number is bad!
            </br> ... the bigger the negative number the worse it is.
            </li>
            <li>
            Use the datepicker(s) to select dates for the period desired
            </li>
            </ul>
            </div>
            
            """.format(self.div_style,self.header_style)
            div = Div(text=txt,width=width,height=height)
            return div

        def stats_information_div(self, width=400, height=300):
            txt = """
            <div {}>
                   <h4 {}>Metadata Info </h4>
                   <ul>
                   <li >
                   <h4 style='margin-bottom:-2px;'>Table left:</h4>
                   - shows the outcome,</br>
                     and the error (difference between prediction and reality)</br>
                     <strong><i>Smaller is better!</i></strong>
                   </li>
                   <li>
                   <h4 style='margin-bottom:-2px;'>Table right:</h4>
                     - shows the desired outcome, the variables(things Aion controls)
                   </br> and their importance to the particular outcome
                   </br> Use the 'rank ...' to tell (smaller is better):
                   </br> ...which variable(s) have a greater impact on an outcome.
                   </li>
                   </ul>
            </div>""".format(self.div_style, self.header_style)
            div = Div(text=txt, width=width, height=height)
            return div

        # use address to predict
        def make_network_prediction(self, start_date, end_date, addresses=None):
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
                block_size, block_time, difficulty, nrg_limit, nrg_reward, num_transactions, \
                block_nrg_consumed, transaction_nrg_consumed, nrg_price, value = \
                    dd.compute(df.block_size.mean(), df.block_time.mean(), df.difficulty.mean(),
                               df.nrg_limit.mean(), df.nrg_reward.mean(), df.num_transactions.mean(),
                               df.block_nrg_consumed.mean(), df.transaction_nrg_consumed.mean(),
                               df.nrg_price.mean(),df.value.mean())

                X = [[round(block_size), round(block_time), round(difficulty), round(nrg_limit),
                     round(nrg_reward), round(num_transactions),
                     round(block_nrg_consumed), round(transaction_nrg_consumed),
                     round(nrg_price), round(value)]]

                predictions_lst = []
                for target in self.targets:
                    predictions_lst.append(self.pl[target].predict(X))
                    logger.warning('MAKE PREDICTIONS COMPLETED FOR :%s', target)
                df = pd.DataFrame(
                    {'Outcome': self.targets,
                     '#': predictions_lst,
                     })
                return df.hvplot.table(columns=['Outcome','#'],width=500,
                                       title='Predictions for accounts retained and churned')
            except Exception:
                logger.error("MAKE PREDICTIONS:", exc_info=True)

        def rf_clf(self):
            try:
                logger.warning("RANDOM FOREST LAUNCHED")
                groupby_dict = {
                    'value': 'mean',
                    'block_nrg_consumed': 'mean',
                    'transaction_nrg_consumed': 'mean',
                    'difficulty': 'mean',
                    'nrg_limit': 'mean',
                    'block_size': 'mean',
                    'block_time': 'mean',
                    'nrg_reward': 'mean'
                }
                self.df_grouped = self.group_data(self.df,groupby_dict)

                X = self.df_grouped.drop(['churned', 'churned_verbose',
                                          self.interest_var, ], axis=1)

                y = self.df_grouped['churned']

                X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3)
                self.clf = RandomForestClassifier(n_estimators=200, random_state=42,
                                             class_weight="balanced")

                self.feature_list = X_train.columns.tolist()

                self.clf.fit(X_train, y_train)
                y_pred = self.clf.predict(X_test)

                acc_text = """
                <div{}>
                <h4 {}>Predictive accuracy:</h4><strong 'style=color:black;'>{}</strong>""" \
                    .format(self.div_acc_style,
                            self.header_style,
                            metrics.accuracy_score(y_test, y_pred))

                self.make_tree(self.clf)

                self.metrics_div.text = acc_text
                print('confusion matrix:\n')
                print(confusion_matrix(y_test, y_pred))
                print('classification report:\n')
                print(classification_report(y_test, y_pred))
            except Exception:
                logger.error("RF:", exc_info=True)

        # the period for which the user wants a prediction
        def make_account_predictions(self,start_date, end_date,addresses):
            try:
                logger.warning("MAKE PREDICTIONS LAUNCHED")

                # make
                if not self.clf:
                    self.rf_clf()

                if self.load_data_flag:
                    df = self.cl.load_data('block_tx_warehouse', self.feature_list, start_date, end_date)

                # filter if prediction for certain addresses
                if addresses is not None:
                    if addresses not in ['all', '']:
                        df = df[df['address'] == addresses]

                logger.warning('line 408 predict-df post filter:%s', df.head(20))

                if len(df) > 0:

                    # run model
                    df = df.fillna(0)
                    X = df.drop([self.interest_var], axis=1)
                    interest_labels = df[self.interest_var].tolist()
                    logger.warning("lengths of df:%s,lst:%s", len(df), len(interest_labels))
                    # logger.warning("df before prediction:%s",X.tail(10))

                    y_pred = self.clf.predict(X)
                    y_pred_verbose = ['to leave' if x in ["1", 1] else "to remain" for x in y_pred]
                    # make table for display
                    self.predict_df = pd.DataFrame({
                        'address': interest_labels,
                        'likely...': y_pred_verbose
                    })
                    perc_to_churn = round(100 * sum(y_pred) / len(y_pred), 1)
                    text = self.metrics_div.text + """
                    <br/> <h3{}>Percentage likely to churn:</h3>
                    <strong 'style=color:black;'>{}%</strong></div>""".format(self.header_style,
                                                                              perc_to_churn)
                    self.metrics_div.text = text
                else:
                    # make table for display
                    self.predict_df = pd.DataFrame({
                        'address': [],
                        'likely...': []
                    })
                    text = self.metrics_div.text + """
                        <br/> <h3{}>Sorry, address not found</h3>
                        <strong 'style=color:black;'>{}%</strong></div>""".format(self.header_style)
                    self.metrics_div.text = text
                logger.warning("end of predictions")
                return df.hvplot.table(columns=['address', 'likely'], width=500,
                                       title='Churn predictions for accounts')
            except Exception:
                logger.error("prediction:", exc_info=True)

        def make_tree(self,target='churned_diff'):
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
                #filepath = self.make_filepath('../../../static/images/small_tree.gif')
                #.write_png(filepath)
                filepath = self.make_filepath('/home/andre/Downloads/small_tree.png')
                graph.write_png(filepath)
                logger.warning("TREE SAVED")


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
                    'rank_within_outcome':[]
                }
                for target in self.targets:
                    logger.warning('make feature importances for :%s',target)
                    # Get numerical feature importances
                    importances = list(self.pl[target].named_steps['rf'].feature_importances_)

                    # List of tuples with variable and importance
                    feature_importances = [(feature, round(importance, 4)) for feature, importance in
                                           zip(self.feature_list, importances)]

                    sorted_importances = sorted(feature_importances, key=itemgetter(1))

                    #logger.warning('importances :%s',importances)
                    #logger.warning("feature_importances:%s",feature_importances)
                    target_lst = [target] * len(importances)

                    count = 1
                    rank_lst = []
                    for i in importances:
                        rank_lst.append(str(count))
                        count += 1

                    results_dct['outcome'] += target_lst
                    results_dct['feature'] += [i[0] for i in sorted_importances]
                    results_dct['importance'] += [i[1] for i in sorted_importances]
                    results_dct['rank_within_outcome'] += rank_lst

                df = pd.DataFrame.from_dict(results_dct)
                logger.warning('MAKE FEATURE IMPORTANCES FINISHED')
                return df.hvplot.table(columns=['outcome','feature','importance', 'rank_within_outcome'],
                                       width=600,
                                       title="Variables ranked by importance (for each output)")

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
                return df.hvplot.box(variable, by='day_of_week',ylim=(0,600),width=600)
            except Exception:
                logger.error('dow', exc_info=True)


        def tree_div(self, width=1000, height=600, path='/static/images/small_tree.png'):
            self.make_tree()
            txt = """
            <h3 {}> A decision tree for churn: </h3>
            <img src='../../../static/small_tree.png' />
            """.format(self.header_style)
            return Div(text=txt,width=width,height=height)

    def update(attrname, old, new):
        this_tab.notification_updater("Calculations underway. Please be patient")
        this_tab.load_data_flag = True # only load prediction period data once
        this_tab.prediction_address_selected = this_tab.prediction_address_select.value
        stream_start_date.event(start_date=datepicker_start.value)
        stream_end_date.event(end_date=datepicker_end.value)
        this_tab.load_data_flag = False
        this_tab.notification_updater("")

    def update_account_predictions(attrname, old, new):
        this_tab.notification_updater("Calculations underway. Please be patient")
        this_tab.prediction_address_selected = this_tab.prediction_address_select.value
        stream_start_date.event(start_date=datepicker_start.value)
        stream_end_date.event(end_date=datepicker_end.value)
        this_tab.notification_updater("")


    try:
        # SETUP
        table = 'account_activity_churn'
        cols = list(table_dict[table].keys())
        cols = ['new','retained','churned','day_of_week','block_size', 'block_timestamp',
                'block_time','difficulty', 'nrg_limit',
                'nrg_reward' , 'num_transactions','block_nrg_consumed','nrg_price',
                'transaction_nrg_consumed','churned_lst','retained_lst','value']
        this_tab = Thistab(table,cols,[])
        this_tab.load_df()
        cols1 = ['retained','churned']
        cols3 = ['retained', 'churned','day_of_week']

        cols1_diff = ['retained', 'churned']

        # setup dates
        first_date_range = datetime.strptime("2018-04-23 00:00:00", "%Y-%m-%d %H:%M:%S")
        last_date_range = datetime.now().date()
        range = 20
        first_date = datetime_to_date(last_date_range - timedelta(days=range))

        # STREAMS Setup
        # date comes out stream in milliseconds
        stream_start_date = streams.Stream.define('Start_date',
                                                  start_date=first_date)()
        stream_end_date = streams.Stream.define('End_date',
                                                end_date=last_date_range)()
        stream_address = streams.Stream.define('Address_selected',
                                               addresses=['all'])

        # setup widgets
        datepicker_start = DatePicker(title="Start", min_date=first_date_range,
                                      max_date=last_date_range, value=first_date)
        datepicker_end = DatePicker(title="End", min_date=first_date_range,
                                    max_date=last_date_range, value=last_date_range)
        # search by address checkboxes
        this_tab.prediction_address_select = Select(
            title='Filter by address',
            value='all',
            options=this_tab.address_list)
        reset_prediction_address_button = Button(label="reset address(es)",button_type="success")


        #  MAKE GRAPHS
        # simple history
        hv_history_curves1 = hv.DynamicMap(this_tab.history_line_graphs(cols1)).\
            opts(plot=dict(width=600, height=400))
        history_curves1 = renderer.get_plot(hv_history_curves1)

        # percentage difference history
        hv_history_curves1_diff = hv.DynamicMap(this_tab.history_line_graphs(cols1_diff)). \
            opts(plot=dict(width=600, height=400))
        history_curves1_diff = renderer.get_plot(hv_history_curves1_diff)


        # tables
        hv_accuracy_table = hv.DynamicMap(this_tab.rf_table)
        accuracy_table = renderer.get_plot(hv_accuracy_table)

        hv_network_prediction_table = hv.DynamicMap(this_tab.make_network_prediction,
                                                    streams=[stream_start_date,stream_end_date])
        network_prediction_table = renderer.get_plot(hv_network_prediction_table)

        '''
        hv_account_prediction_table = hv.DynamicMap(this_tab.make_account_predictions,
                                                    streams=[stream_start_date, stream_end_date,
                                                             stream_address])
        account_prediction_table = renderer.get_plot(hv_account_prediction_table)
        '''


        hv_features_table = hv.DynamicMap(this_tab.make_feature_importances)
        features_table = renderer.get_plot(hv_features_table)

        # split by dow
        hv_dow1= hv.DynamicMap(this_tab.dow(cols3,'churned'))
        dow1 = renderer.get_plot(hv_dow1)

        hv_dow3 = hv.DynamicMap(this_tab.dow(cols3,'retained'))
        dow3 = renderer.get_plot(hv_dow3)


        # add callbacks
        datepicker_start.on_change('value', update)
        datepicker_end.on_change('value', update)
        this_tab.prediction_address_select.on_change('value',update_account_predictions)
        reset_prediction_address_button.on_click(this_tab.reset_checkboxes)


        # put the controls in a single element
        date_controls = WidgetBox(datepicker_start, datepicker_end,
                                  this_tab.prediction_address_select,
                                  reset_prediction_address_button)

        grid = gridplot([
            [this_tab.notification_div],
            [this_tab.title_div('Churned and new aioners by date'),this_tab.title_div('Rolling % daily difference for '
                                                                                      'churned and new aioners')],
            [history_curves1.state,history_curves1_diff.state],
            [this_tab.title_div('Distribution of churned and new aioners by day of week')],
            [dow1.state,dow3.state],
            [this_tab.title_div('Prediction stats for new,churned,aioners models ',600)],
            [accuracy_table.state,this_tab.stats_information_div(),features_table.state],
            [this_tab.title_div('Select period below to obtain predictions:', 600)],
            [date_controls, this_tab.prediction_information_div(), network_prediction_table.state],
            [this_tab.notification_div_bottom]
        ])

        tab = Panel(child=grid, title='Account activity predictions')
        return tab

    except Exception:
        logger.error('rendering err:', exc_info=True)
        text = 'network activity predictions'
        return tab_error_flag(text)
