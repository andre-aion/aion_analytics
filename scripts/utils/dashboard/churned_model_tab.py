from scripts.utils.mylogger import mylogger
from scripts.utils.modelling.churned import find_in_redis,\
    construct_from_redis, extract_data_from_dict
from scripts.utils.myutils import tab_error_flag
from scripts.utils.dashboard.mytab import Mytab
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


import holoviews as hv
from tornado.gen import coroutine

lock = Lock()
executor = ThreadPoolExecutor()
logger = mylogger(__file__)

hv.extension('bokeh', logo=False)


class ChurnedModelTab:
    def __init__(self,tier=1,cols=[]):
        self.tier = tier
        self.checkbox_group = None
        self.cols = cols
        self.df = None
        self.churned_list = None
        self.retained_list = None

    # show checkbox list of reference periods produced by the churn tab
    def make_checkboxes(self):
        try:
            # make list of
            lst = find_in_redis()
            self.checkbox_group = CheckboxGroup(labels=lst,
                                           active=[1])
        except Exception:
            logger.error('make checkboxes',exc_info=True)

    def make_modelling_button(self):
        try:
            # make list of
            button = Button(label="Launch modelling", button_type="success")
            return button
        except Exception:
            logger.error('make modelling button', exc_info=True)

    def load_data(self):
        try:
            dict_lst = [self.checkbox_group.labels[i] for i in self.checkbox_group.active]
            self.df, self.churned_list, self.retained_list = extract_data_from_dict(dict_lst,self.cols)
            # clear notification message
        except Exception:
            logger.error('make modelling button', exc_info=True)

    def label_state(self, x):
        if x in self.churned_list:
            return 'churned'
        return 'retained'

    def label_churned_retained(self):
        if self.tier == 1:
            search_col = 'from_addr'
        else:
            search_col = 'to_addr'
        self.df['state'] = self.df[search_col] \
            .map(self.label_state)


    def notification_updater(self, text):
        return '<h3  style="color:red">{}</h3>'.format(text)
