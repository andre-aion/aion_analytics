from scripts.utils.mylogger import mylogger
from scripts.utils.dashboard.churned_model_tab import ChurnedModelTab
from scripts.utils.myutils import tab_error_flag
from scripts.utils.dashboard.mytab import Mytab
from concurrent.futures import ThreadPoolExecutor
from tornado.locks import Lock

from bokeh.layouts import gridplot, WidgetBox
from bokeh.models import ColumnDataSource, Panel
from bokeh.models.widgets import  Div, \
    DatePicker, Select

from datetime import datetime

import holoviews as hv
from tornado.gen import coroutine

lock = Lock()
executor = ThreadPoolExecutor()
logger = mylogger(__file__)

hv.extension('bokeh', logo=False)

@coroutine
def churned_model(tier=1):
    class Thistab(ChurnedModelTab):

        def __init__(self):
            ChurnedModelTab.__init__(self)

    try:
        thistab = Thistab()
        notification_text = thistab.notification_updater("Hello")
        notification_div = Div(text=notification_text, width=500, height=50)


        checkbox_group = thistab.make_checkboxes()

        grid = gridplot([[checkbox_group],
                         [notification_div],
                        ])

        tab = Panel(child=grid, title='Tier '+str(tier)+' pre churn model ')
        return tab


    except Exception:
        logger.error('rendering err:',exc_info=True)
        return tab_error_flag('tier1_churned_model')




