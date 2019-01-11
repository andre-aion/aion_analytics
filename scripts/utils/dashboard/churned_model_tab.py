from scripts.utils.mylogger import mylogger
from scripts.utils.modelling.churned import find_in_redis,\
    construct_from_redis
from scripts.utils.myutils import tab_error_flag
from scripts.utils.dashboard.mytab import Mytab
from concurrent.futures import ThreadPoolExecutor
from tornado.locks import Lock

from bokeh.layouts import gridplot, WidgetBox
from bokeh.models import ColumnDataSource, Panel
from bokeh.models.widgets import  Div, \
    DatePicker, Select, CheckboxGroup

from datetime import datetime

import holoviews as hv
from tornado.gen import coroutine

lock = Lock()
executor = ThreadPoolExecutor()
logger = mylogger(__file__)

hv.extension('bokeh', logo=False)


class ChurnedModelTab:
    def __init__(self,tier=1):
        self.tier = tier

    # show checkbox list of reference periods produced by the churn tab
    def make_checkboxes(self):
        try:
            # make list of
            lst = find_in_redis()
            checkbox_group = CheckboxGroup(labels=lst,
                                           active=[1])
            return checkbox_group
        except Exception:
            logger.error('make checkboxes',exc_info=True)

    def notification_updater(self, text):
        return '<h3  style="color:red">{}</h3>'.format(text)
