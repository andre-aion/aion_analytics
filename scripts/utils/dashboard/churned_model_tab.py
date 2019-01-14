import time
from os.path import dirname, join

from scripts.utils.mylogger import mylogger
from scripts.utils.modelling.churned import find_in_redis,\
    construct_from_redis, extract_data_from_dict, get_miner_list
from scripts.utils.dashboard.mytab import Mytab
from scripts.streaming.streamingDataframe import StreamingDataframe as SD

import dask.dataframe as dd

from concurrent.futures import ThreadPoolExecutor
from tornado.locks import Lock

from bokeh.layouts import gridplot, WidgetBox
from bokeh.models import ColumnDataSource, Panel
from bokeh.models.widgets import Div, \
    DatePicker, Select, CheckboxGroup, Button

from datetime import datetime
import gc
from bokeh.models.widgets import Div, Select, \
    DatePicker, TableColumn, DataTable
from holoviews import streams

import hvplot
import pandas as pd
from scipy import stats
import numpy as np

import holoviews as hv
from tornado.gen import coroutine
from sklearn.model_selection import train_test_split
from sklearn.svm import SVC
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.ensemble import RandomForestClassifier
from sklearn import metrics

lock = Lock()

executor = ThreadPoolExecutor()
logger = mylogger(__file__)

hv.extension('bokeh', logo=False)
hyp_variables = [ 'block_number','block_nrg_consumed', 'transaction_nrg_consumed', 'approx_value',
                 'difficulty', 'nrg_price', 'nrg_limit', 'block_size', 'approx_nrg_reward'
                ]

class ChurnedModelTab:
    def __init__(self,tier=1,cols=[]):
        self.tier = tier
        self.checkbox_group = None
        self.churned_list = None
        self.retained_list = None
        self.df = SD('block_tx_warehouse', cols, dedup_columns=[]).get_df()
        self.max = 10
        self.select_variable = None
        self.df1 = {}
        self.df_grouped = ''
        self.interest_var = 'from_addr'
        self.counting_var = 'to_addr'
        self.cols = cols
        self.notification_div = Div(text='')
        self.clf = None
        self.poolname_dict = self.get_poolname_dict()
        self.metrics_div = Div(text='')
        self.predict_df = SD('predict_table',['address','likely...'], dedup_columns=[]).get_df()
        self.load_data_flag = True
        self.threshold_blocks_mined = 0.5
        self.threshold_tx_paid_out = 5
        self.start_date = datetime.strptime("2018-12-15 00:00:00", '%Y-%m-%d %H:%M:%S')
        self.end_date = datetime.strptime("2018-12-31 00:00:00", '%Y-%m-%d %H:%M:%S')

        if tier == 2:
            self.interest_var = 'to_addr'
            self.counting_var = 'from_addr'

    # show checkbox list of reference periods produced by the churn tab

    def make_checkboxes(self):
        try:
            # make list of
            lst = find_in_redis()
            logger.warning("CHECKBOX LIST:%s", lst)
            if len(lst) < 1:
                active = ''
                lst=[1]
            else:
                active = 0
                self.checkbox_group = CheckboxGroup(labels=lst,
                                                active=[active])
        except Exception:
            logger.error('make checkboxes',exc_info=True)

    def update_checkboxes(self):
        self.checkbox_group.labels = find_in_redis()

    def set_load_data_flag(self, attr, old, new):
        self.load_data_flag = True

    def make_button(self,label):
        try:
            # make list of
            button = Button(label=label, button_type="success")
            return button
        except Exception:
            logger.error('make modelling button', exc_info=True)

    def make_selector(self,title,initial_value):
        try:
            selector = Select(title=title,
                              value=initial_value,
                              options=hyp_variables)
            logger.warning("%s SELECTOR CREATED",initial_value.upper())

            return selector
        except Exception:
            logger.error('make selector', exc_info=True)



    def cast_col(self,column,type):
        try:
            if type =='float':
                self.df[column] = self.df[column].astype(float)
            elif type == 'int':
                self.df[column] = self.df[column].astype(int)
            logger.warning('casted %s as %s',column,type)

        except Exception:
            logger.error('convert string', exc_info=True)

    def group_data(self, df):
        meta = {
            'block_number': 'int',
            'approx_value': 'float',
            'block_nrg_consumed': 'float',
            'transaction_nrg_consumed': 'float',
            'difficulty': 'float',
            'nrg_price': 'float',
            'nrg_limit': 'float',
            'block_size': 'float',
            'block_time': 'float',
            'approx_nrg_reward': 'float',
        }

        for key, value in meta.items():
            self.cast_col(key, value)

        df = df.groupby([self.interest_var]).agg({
            'block_number': 'count',
            self.counting_var: 'count',
            'approx_value': 'mean',
            'block_nrg_consumed': 'mean',
            'transaction_nrg_consumed': 'mean',
            'difficulty': 'mean',
            'nrg_price': 'mean',
            'nrg_limit': 'mean',
            'block_size': 'mean',
            'block_time': 'mean',
            'approx_nrg_reward': 'mean'
        }).compute()

        df = df.reset_index()
        if 'index' in df.columns.tolist():
            df = self.df.drop('index',axis=1)

        return df

    def load_data(self):
        try:
            if self.checkbox_group:
                #reset dataframe to empty
                self.df = SD('block_tx_warehouse', self.cols, dedup_columns=[]).get_df()
                dict_lst = [self.checkbox_group.labels[i] for i in self.checkbox_group.active]
                self.df, self.churned_list, self.retained_list = extract_data_from_dict(
                    dict_lst,self.df)
                self.df = self.df.fillna(0)
                self.df_grouped = self.group_data(self.df)
                self.df_grouped = self.label_churned_retained(self.df_grouped)
                self.df_grouped = self.label_churned_verbose(self.df_grouped)
                self.split_df(self.df_grouped)
                self.load_data_flag = False
                logger.warning('end of load data:%s', self.df_grouped.tail(5))

            # clear notification message
        except Exception:
            logger.error('load data:', exc_info=True)

    def label_state(self, x):
        if x in self.churned_list:
            return 1
        return 0

    def label_state_verbose(self, x):
        if x in self.churned_list:
            return 'churned'
        return 'retained'

    def label_churned_retained(self,df):
        try:
            if df is not None:
                if len(df)>0:
                    df['churned'] = df[self.interest_var] \
                        .map(self.label_state)
                    logger.warning("Finished churned retained")
                    return df
        except Exception:
            logger.error("label churned retained:",exc_info=True)

    def label_churned_verbose(self,df):
        try:
            if df is not None:
                if len(df)>0:
                    df['churned_verbose'] = df[self.interest_var] \
                        .map(self.label_state_verbose)
                    logger.warning("Finished churned retained")
                    return df
        except Exception:
            logger.error("label churned retained:",exc_info=True)

    def get_poolname_dict(self):
        file = join(dirname(__file__), '../../../data/poolinfo.csv')
        df = pd.read_csv(file)
        a = df['address'].tolist()
        b = df['poolname'].tolist()
        poolname_dict = dict(zip(a, b))
        return poolname_dict

    def poolname_verbose(self, x):
        # add verbose poolname
        if x in self.poolname_dict.keys():
            return self.poolname_dict[x]
        return x

    def notification_updater(self, text):
        text = '<h3  style="color:red">{}</h3>'.format(text)
        self.notification_div.text = text

    def results_div(self,text,width=600,height=300):
        div = Div(text=text,width=width,height=height)
        return div

    # PLOTS
    def box_plot(self,variable='approx_value',launch=False):
        try:
            #logger.warning("difficulty:%s", self.df.tail(30))
            #get max value of variable and multiply it by 1.1
            min,max = dd.compute(self.df_grouped[variable].min(),
                                 self.df_grouped[variable].max())
            #logger.warning('df in box plot:%s',self.df_grouped.head(10))
            return self.df_grouped.hvplot.box(variable, by='churned_verbose',
                                              ylim=(.9*min,1.1*max))
        except Exception:
            logger.error("box plot:",exc_info=True)

    def bar_plot(self, variable='approx_value', launch=False):
        try:
            # logger.warning("difficulty:%s", self.df.tail(30))
            # get max value of variable and multiply it by 1.1
            return self.df1.hvplot.bar('miner_address', variable, rot=90,
                                       height=400, width=300, title='block_number by miner address',
                                       hover_cols=['percentage'])
        except Exception:
            logger.error("box plot:", exc_info=True)

    def hist(self,variable='approx_value'):
        try:
            # logger.warning("difficulty:%s", self.df.tail(30))
            # get max value of variable and multiply it by 1.1
            #min, max = dd.compute(self.df_grouped[variable].min(),
                                  #self.df_grouped[variable].max())
            return self.df_grouped.hvplot.hist(
                y=variable, bins=50, by='churned',alpha=0.3)
        except Exception:
            logger.error("box plot:", exc_info=True)

    def split_df(self,df):
        self.df1['churned'] = df[df.churned == 1]
        self.df1['retained'] = df[df.churned == 0]
        logger.warning("Finished split into churned and retained dataframes")

    def convert_to_array(self,df,split,variable):
        try:
            col = df[split][variable].values.tolist()
            #logger.warning("dask array:%s",col)
            logger.warning("Finshed converting %s %s to dask array",variable, split)
            return col
        except Exception:
            logger.error("convert to dask array:", exc_info=True)

    def hypothesis_table(self, launch=False):
        try:
            p_value = []
            impactful=[]
            for variable in hyp_variables:
                try:
                    self.dask_array = {
                        'churned': self.convert_to_array(self.df1,'churned', variable),
                        'retained': self.convert_to_array(self.df1,'retained', variable)
                    }

                    c = self.dask_array['churned']
                    d = self.dask_array['retained']
                    s,p= stats.kruskal(c,d)
                    res = 'yes' if p < 0.05 else 'no'
                    p_value.append(p)
                    impactful.append(res)
                    logger.warning("%s test completed",variable)
                except Exception:
                    logger.error('hypothesis table', exc_info=True)

            df = pd.DataFrame({
                'variable' : hyp_variables,
                'p-value': p_value,
                'impact churn?': impactful
            })
            logger.warning("end of hypothesis test")

            return df.hvplot.table(columns=['variable','p-value','impact churn?'],
                                           width=450)
        except Exception:
            logger.error("hypothesis table:", exc_info=True)

    def svc(self,launch=False):
        try:
            self.notification_updater("svc calculations underway")
            X = self.df_grouped.drop(['churned','churned_verbose',self.interest_var],axis=1)
            y = self.df_grouped['churned']

            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.30)
            svclassifier = SVC(kernel='linear')
            svclassifier.fit(X_train, y_train)
            y_pred = svclassifier.predict(X_test)
            print('confusion matrix:',confusion_matrix(y_test, y_pred))
            print('classification report:',classification_report(y_test, y_pred))
            self.notification_updater("")

        except Exception:
            logger.error("svc:", exc_info=True)

    def rf_clf(self,launch=False):
        try:
            if self.load_data_flag:
                logger.warning('DATA RELOADED TO MAKE PREDICTIONS')
                self.load_data()
            self.notification_updater("svc calculations underway")
            X = self.df_grouped.drop(['churned', 'churned_verbose', self.interest_var], axis=1)
            y = self.df_grouped['churned']
            logger.warning('df in svc:%s', X)

            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25)
            self.clf=RandomForestClassifier(n_estimators=100, random_state=42)
            self.clf.fit(X_train, y_train)
            y_pred = self.clf.predict(X_test)

            acc_text = """
            <h4>Predictive accuracy:</h4>{}""".format(metrics.accuracy_score(y_test, y_pred))

            self.metrics_div.text = acc_text
            print('confusion matrix:\n')
            print(confusion_matrix(y_test, y_pred))
            print('classification report:\n')
            print(classification_report(y_test, y_pred))
            self.notification_updater("")

        except Exception:
            logger.error("svc:", exc_info=True)

    # the period for which the user wants a prediction
    def make_predictions(self):
        try:
            # make model
            self.rf_clf()
            to_predict_tab = Mytab('block_tx_warehouse',cols=self.cols,dedup_cols=[])
            to_predict_tab.df = None
            to_predict_tab.key_tab = 'churn'

            to_predict_tab.df_load(self.start_date,self.end_date)
            logger.warning('predict data before grouping:%s',to_predict_tab.df.head(20))
            df = self.group_data(to_predict_tab.df)

            # filter df for only tier 1/2 miners
            tier_miner_lst = list(set(self.churned_list+self.retained_list))
            df = df[df[self.interest_var].isin(tier_miner_lst)]
            df[self.interest_var] = df[self.interest_var].map(self.poolname_verbose)

            # run model
            df = df.fillna(0)
            X = df.drop([self.interest_var], axis=1)
            interest_labels = df[self.interest_var].tolist()
            logger.warning("lengths of df:%s,lst:%s",len(df),len(interest_labels))
            logger.warning("df before prediction:%s",X.tail(10))

            y_pred = self.clf.predict(X)
            y_pred_verbose = ['to leave' if x in ["1",1] else "to remain" for x in y_pred]
            # make table for display
            self.predict_df = pd.DataFrame({
                'address': interest_labels,
                'likely...': y_pred_verbose
            })
            perc_to_churn = round(100*sum(y_pred)/len(y_pred),1)
            text = self.metrics_div.text + """
            <br/> <h3>Percentage likely to churn:</h3>{}%""".format(perc_to_churn)
            self.metrics_div.text=text
            logger.warning("end of predictions")


        except Exception:
            logger.error("prediction:", exc_info=True)

    def prediction_table(self,launch=False):
        try:
            self.make_predictions()
            return self.predict_df.hvplot.table(columns=['address', 'likely...'],
                                width=800,height=1400)
        except Exception:
            logger.error("prediction table:", exc_info=True)