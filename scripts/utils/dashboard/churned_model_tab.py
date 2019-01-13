import time

from scripts.utils.mylogger import mylogger
from scripts.utils.modelling.churned import find_in_redis,\
    construct_from_redis, extract_data_from_dict
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

lock = Lock()
executor = ThreadPoolExecutor()
logger = mylogger(__file__)

hv.extension('bokeh', logo=False)
hyp_variables = ['block_nrg_consumed', 'transaction_nrg_consumed', 'approx_value',
                 'difficulty', 'nrg_price', 'nrg_limit', 'block_size', 'approx_nrg_reward',
                 'block_number']

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
        if tier == 2:
            self.interest_var = 'to_addr'
            self.counting_var = 'from_addr'

    # show checkbox list of reference periods produced by the churn tab

    def make_checkboxes(self):
        try:
            # make list of
            lst = find_in_redis()
            logger.warning("CHECKBOX LIST:%s", lst)
            if len(lst) <= 1:
                active = ''
            else:
                active = 1
            self.checkbox_group = CheckboxGroup(labels=lst,
                                                active=[active])
        except Exception:
            logger.error('make checkboxes',exc_info=True)

    def update_checkboxes(self):
        self.checkbox_group.labels = find_in_redis()

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


    def load_data(self):
        try:
            if self.checkbox_group:
                #reset dataframe to empty
                self.df = SD('block_tx_warehouse', self.cols, dedup_columns=[]).get_df()
                dict_lst = [self.checkbox_group.labels[i] for i in self.checkbox_group.active]
                self.df, self.churned_list, self.retained_list = extract_data_from_dict(
                    dict_lst,self.df)
                self.df = self.df.fillna(0)

                meta={
                    'approx_value': 'float',
                    'block_nrg_consumed':'float',
                    'transaction_nrg_consumed': 'float',
                    'difficulty': 'float',
                    'nrg_price': 'float',
                    'nrg_limit': 'float',
                    'block_size':'float',
                    'block_time':'float',
                    'approx_nrg_reward':'float',
                    'block_number':'int'}

                for key, value in meta.items():
                    self.cast_col(key,value)

                self.df_grouped = self.df.groupby([self.interest_var])\
                    .agg({
                        'approx_value': 'mean',
                        'transaction_nrg_consumed':'mean',
                        'block_nrg_consumed':'mean',
                        'difficulty': 'mean',
                        'nrg_price': 'mean',
                        'nrg_limit': 'mean',
                        'block_size':'mean',
                        'block_time':'mean',
                        self.counting_var:'count',
                        'approx_nrg_reward':'mean',
                        'block_number': 'count'
                    }).compute()

                self.df_grouped = self.df_grouped.reset_index()
                #self.df_grouped = self.df_grouped.drop('index',axis=1)
                self.label_churned_retained(self.df_grouped)
                self.label_churned_verbose(self.df_grouped)
                self.split_df(self.df_grouped)

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
            if len(df)>0:
                df['churned'] = df[self.interest_var] \
                    .map(self.label_state)
            logger.warning("Finished churned retained")
        except Exception:
            logger.error("label churned retained:",exc_info=True)

    def label_churned_verbose(self,df):
        try:
            if len(df)>0:
                df['churned_verbose'] = df[self.interest_var] \
                    .map(self.label_state_verbose)
            logger.warning("Finished churned retained")
        except Exception:
            logger.error("label churned retained:",exc_info=True)

    def notification_updater(self, text):
        return '<h3  style="color:red">{}</h3>'.format(text)

    def results_div(self,text,width=300,height=300):
        div = Div(text=text,width=width,height=height)
        return div

    # PLOTS
    def box_plot(self,variable='approx_value',launch=False):
        try:
            #logger.warning("difficulty:%s", self.df.tail(30))
            #get max value of variable and multiply it by 1.1
            min,max = dd.compute(self.df_grouped[variable].min(),
                                 self.df_grouped[variable].max())
            logger.warning('df in box plot:%s',self.df_grouped.head(10))
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
            min, max = dd.compute(self.df_grouped[variable].min(),
                                  self.df_grouped[variable].max())
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

    '''
    def hypothesis_test(self, variable):
        try:
            logger.warning("starting hypothesis test")

            self.dask_array = {
                'churned' : self.convert_to_dask_array('churned',variable),
                'retained' : self.convert_to_dask_array('retained',variable)
            }
            # check variance of two populations to see if they are true
            #c = np.asarray(self.dask_array['churned'])
            #d = np.asarray(self.dask_array['retained'])
            c = self.dask_array['churned']
            d = self.dask_array['retained']
            a = nanvar(c)
            b = nanvar(d)
            equal_var = isclose(a, b, abs_tol=10**-8)
            res = stats.ttest_ind(a,b,equal_var=equal_var)
            s,p = res.compute()
            res = 'yes' if p < 0.05 else 'no'
            text = """
            <p> 
            <h3>{}</h3></br>
            p-value:{}
            <br/>
            Is there a statistically significant difference <br/>
            between churners and remainers?:{}</p>
            """.format(variable,p,res.upper())
            logger.warning("hypothesis test completed")

            return text

        except Exception:
            logger.error("hypothesis test:", exc_info=True)
    '''
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
                                           width=300)
        except Exception:
            logger.error("hypothesis table:", exc_info=True)

