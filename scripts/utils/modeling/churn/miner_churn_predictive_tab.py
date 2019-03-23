import time
from os.path import dirname, join

import pydot
from sklearn.tree import export_graphviz

from scripts.utils.mylogger import mylogger
from scripts.utils.modeling.churn.miner_predictive_methods import extract_data_from_dict
from scripts.utils.dashboards.EDA.mytab_interface import Mytab
from scripts.streaming.streamingDataframe import StreamingDataframe as SD

import dask.dataframe as dd

from concurrent.futures import ThreadPoolExecutor
from tornado.locks import Lock

from datetime import datetime, date, timedelta, time

import pandas as pd

import holoviews as hv
from sklearn.model_selection import train_test_split
from sklearn.svm import SVC
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.ensemble import RandomForestClassifier
from sklearn import metrics

from scripts.utils.myutils import make_filepath

lock = Lock()

executor = ThreadPoolExecutor()
logger = mylogger(__file__)

hv.extension('bokeh', logo=False)
hyp_variables = ['block_nrg_consumed', 'transaction_nrg_consumed', 'value',
                 'difficulty','nrg_limit', 'block_size', 'nrg_reward'
                ]

class MinerChurnPredictiveTab:
    def __init__(self,tier=1,cols=[]):
        self.tier = tier
        self.churned_list = None
        self.retained_list = None
        self.tab = Mytab('block_tx_warehouse', cols, [])
        self.df = self.tab.df
        self.max = 10
        self.select_variable = None
        self.df1 = {}
        self.day_diff = 1
        self.df_grouped = ''
        self.cols = cols
        self.clf = None
        self.poolname_dict = self.get_poolname_dict()
        # DIV CREATION
        self.feature_list = []
        self.info_color = 'mediumseagreen'
        self.div_style = """ style='width:300px; margin-left:25px;
                          border:1px solid #ddd;border-radius:3px;background:#efefef50;' 
                          """
        self.div_acc_style = """ style='color:LightSteelBlue;width:300px; margin-left:25px;
                                 border:1px solid #ddd;border-radius:3px;background:#efefef50;' 
                                 """
        self.header_style = """ style='color:blue;text-align:center;' """


        self.predict_df = SD('predict_table',['address','likely...'], dedup_columns=[]).get_df()
        self.load_data_flag = True
        self.threshold_tx_received = 1
        self.threshold_blocks_mined = 5
        self.threshold_tx_paid_out = 1
        self.start_date = datetime.strptime("2018-12-15 00:00:00", '%Y-%m-%d %H:%M:%S')
        self.end_date = datetime.strptime("2018-12-31 00:00:00", '%Y-%m-%d %H:%M:%S')

        self.hyp_variables = hyp_variables

        if tier in ["2", 2]:
            self.interest_var = 'to_addr'
            self.counting_var = 'from_addr'
        else:
            self.interest_var = 'from_addr'
            self.counting_var = 'to_addr'
        # list of tier specific addresses for prediction
        self.address_list = []
        self.prediction_address_selected = ""

    def reset_checkboxes(self):
        try:
            self.prediction_address_selected = ""
            self.prediction_address_select.value = "all"
        except Exception:
            logger.error('reset checkboxes', exc_info=True)

    def set_load_data_flag(self, attr, old, new):
        self.load_data_flag = True
        self.load_data_flag = True


    def cast_col(self,column,type):
        try:
            if type =='float':
                self.df[column] = self.df[column].astype(float)
            elif type == 'int':
                self.df[column] = self.df[column].astype(int)
            #logger.warning('casted %s as %s',column,type)

        except Exception:
            logger.error('convert string', exc_info=True)

    def group_data(self, df):
        meta = {

            'value': 'float',
            'block_nrg_consumed': 'int',
            'transaction_nrg_consumed': 'int',
            'difficulty': 'int',
            'nrg_limit': 'int',
            'block_size': 'int',
            'block_time': 'int',
            'nrg_reward': 'float',
        }

        for key, value in meta.items():
            self.cast_col(key, value)

        # normalize columns by number of days under consideration
        df = self.normalize(df)
        df = df.groupby([self.interest_var]).agg({
            'value': 'mean',
            'block_nrg_consumed': 'mean',
            'transaction_nrg_consumed': 'mean',
            'difficulty': 'mean',
            'nrg_limit': 'mean',
            'block_size': 'mean',
            'block_time': 'mean',
            'nrg_reward': 'mean'
        }).compute()

        df = df.reset_index()
        if 'index' in df.columns.tolist():
            df = df.drop('index',axis=1)
        df = df.fillna(0)
        #logger.warning('df after groupby:%s', self.df.head(10))

        return df

    def divide_by_day_diff(self,x):
        y = x/self.day_diff
        logger.warning('Normalization:before:%s,after:%s',x,y)
        return y

    """  daily normalization by dividing each column by number of 
      days spanned by a dataframe """
    def normalize(self, df):
        try:
            min_date, max_date = dd.compute(df.block_timestamp.min(),
                                            df.block_timestamp.max())
            self.day_diff = abs((max_date - min_date).days)
            logger.error("NORMALIZATION started for day-diff:%s day(s)",self.day_diff)
            if self.day_diff > 0:
                for col in df.columns:
                    if isinstance(col,int) or isinstance(col,float):
                        logger.warning("NORMALATION ONGOING FOR %s",col)
                        df[col] = df[col].map(self.divide_by_day_diff)
            logger.warning("NORMALIZATION ended for day-diff:%s days",self.day_diff)
            return df
        except Exception:
            logger.error('nomalize:',exc_info=True)


    def load_data(self):
        try:
            if self.checkbox_group:
                #reset dataframe to empty
                self.df = SD('block_tx_warehouse', self.cols, dedup_columns=[]).get_df()
                dict_lst = []
                logger.warning("tier 2 checkbox group active:%s",self.checkbox_group.active)
                if len(self.checkbox_group.active) > 0:
                    if len(self.checkbox_group.labels) >  0:
                        dict_lst = [self.checkbox_group.labels[i] for i in self.checkbox_group.active]
                    else:
                        self.checkbox_group.active = []
                self.df, self.churned_list, self.retained_list = extract_data_from_dict(
                    dict_lst, self.df, tier=self.tier)

                if self.df is not None:
                    if len(self.df) > 0:
                        self.df = self.df.fillna(0)
                        self.df_grouped = self.group_data(self.df)
                        # make list of address for prediction select
                        self.address_list = ['all']+self.df_grouped[self.interest_var].unique().tolist()
                        self.df_grouped = self.label_churned_retained(self.df_grouped)
                        self.df_grouped = self.label_churned_verbose(self.df_grouped)
                        self.split_df(self.df_grouped)
                        self.load_data_flag = False
                #logger.warning('end of load data:%s', self.df_grouped.tail(5))

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
        file = join(dirname(__file__), '../../../../data/poolinfo.csv')
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


    def svc(self,launch=False):
        try:
            X = self.df_grouped.drop(['churned','churned_verbose',self.interest_var],axis=1)
            y = self.df_grouped['churned']

            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.30)
            svclassifier = SVC(kernel='linear')
            svclassifier.fit(X_train, y_train)
            y_pred = svclassifier.predict(X_test)
            print('confusion matrix:',confusion_matrix(y_test, y_pred))
            print('classification report:',classification_report(y_test, y_pred))

        except Exception:
            logger.error("svc:", exc_info=True)

    def rf_clf(self):
        try:
            logger.warning("RANDOM FOREST LAUNCHED")

            if self.load_data_flag:
                logger.warning('DATA RELOADED TO MAKE PREDICTIONS')
                self.load_data()

            X = self.df_grouped.drop(['churned','churned_verbose',
                                      self.interest_var,], axis=1)

            y = self.df_grouped['churned']

            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3)
            clf=RandomForestClassifier(n_estimators=200, random_state=42,
                                       class_weight="balanced")

            self.feature_list = X_train.columns.tolist()

            clf.fit(X_train, y_train)
            y_pred = clf.predict(X_test)

            acc_text = """
            <div{}>
            <h4 {}>Predictive accuracy:</h4><strong 'style=color:black;padding-top:-10px;'>{}</strong>"""\
                .format(self.div_acc_style,
                self.header_style,
                metrics.accuracy_score(y_test, y_pred))


            self.make_tree(clf)

            self.metrics_div.text = acc_text
            print('confusion matrix:\n')
            print(confusion_matrix(y_test, y_pred))
            print('classification report:\n')
            print(classification_report(y_test, y_pred))
            return clf
        except Exception:
            logger.error("RF:", exc_info=True)

    def make_tree(self,clf):
        try:
            if clf is not None:
                logger.warning("INSIDE TREE SAVED")
                # Limit depth of tree to 3 levels
                # Extract the small tree
                tree_small = clf.estimators_[5]
                # Save the tree as a png image
                export_graphviz(tree_small, out_file='small_tree.dot',
                                feature_names=self.feature_list, rounded=True,
                                precision=1)

                (graph,) = pydot.graph_from_dot_file('small_tree.dot')
                # filepath = self.make_filepath('../../../static/images/small_tree.gif')
                # .write_png(filepath)
                filepath = make_filepath(path='/home/andre/Downloads/tier1_tree.png')
                graph.write_png(filepath)
                logger.warning("TREE SAVED")


        except Exception:
            logger.error("make tree:", exc_info=True)


    # the period for which the user wants a prediction
    def make_predictions(self):
        try:
            logger.warning("MAKE PREDICTIONS LAUNCHED")

            # make
            clf = self.clf
            if not clf:
                clf = self.rf_clf()

            if clf is not None:
                to_predict_tab = Mytab('block_tx_warehouse', cols=self.cols, dedup_cols=[])
                to_predict_tab.df = None
                to_predict_tab.key_tab = 'churn'
                logger.warning('LOADING PREDICT WAREHOUSE %s : %s',self.start_date,self.end_date)
                if isinstance(self.end_date,date):
                    mintime = time(00,00,00)
                    self.end_date = datetime.combine(self.end_date,mintime)
                self.end_date = self.end_date + timedelta(days=1)
                to_predict_tab.df_load(self.start_date,self.end_date)
                df = self.group_data(to_predict_tab.df)
                # filter if prediction for certain addresses necessary
                address = self.prediction_address_selected
                logger.warning('line 408 address pre filter:%s',address)
                if address not in ['all','']:
                    df = df[df[self.interest_var] == address]

                logger.warning('line 408 predict-df post filter:%s',df.head(20))

                if len(df)>0:
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

                    y_pred = clf.predict(X)
                    y_pred_verbose = ['to leave' if x in ["1",1] else "to remain" for x in y_pred]
                    # make table for display
                    self.predict_df = pd.DataFrame({
                        'address': interest_labels,
                        'likely...': y_pred_verbose
                    })
                    perc_to_churn = round(100*sum(y_pred)/len(y_pred),1)
                    text = self.metrics_div.text + """
                    <br/> <h3{}>Percentage likely to churn:</h3>
                    <strong 'style=color:black;padding-top:-10px;'>{}%</strong></div>""".format(self.header_style,
                                                                                    perc_to_churn)
                    self.metrics_div.text=text
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
        except Exception:
            logger.error("prediction:", exc_info=True)


