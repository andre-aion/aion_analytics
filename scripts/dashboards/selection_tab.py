from os.path import join, dirname

from scripts.utils.mylogger import mylogger
from scripts.utils.dashboards.poolminer import make_tier1_list,\
    make_tier2_list, is_tier2_in_memory, is_tier1_in_memory
from scripts.utils.myutils import tab_error_flag, datetime_to_date
from scripts.utils.dashboards.mytab_blockminer import MytabPoolminer
from concurrent.futures import ThreadPoolExecutor
from tornado.locks import Lock

from bokeh.layouts import gridplot, WidgetBox
from bokeh.models import ColumnDataSource, Panel, CustomJS
import gc
from bokeh.models.widgets import Div, \
    DatePicker, TableColumn, DataTable, Button, Select, CheckboxGroup

from datetime import datetime, timedelta

import holoviews as hv
from tornado.gen import coroutine

lock = Lock()
executor = ThreadPoolExecutor()
logger = mylogger(__file__)

hv.extension('bokeh', logo=False)


labels = [
    'blockminer_tab',
    'poolminer_tab',
    'churn_tab',
    'tier1_miner_churn_predictive_tab',
    'tier2_miner_churn_predictive_tab',
    'network_activity_predictive_tab']


class SelectionTab:
    selection_checkboxes = CheckboxGroup(labels=labels,active=[0])

    def __init__(self):
        self.selected_tabs = [self.selection_checkboxes.labels[i] for i in
                         self.selection_checkboxes.active]
        self.notification_div = Div(text="""<h2 style='color:green;'>Welcome. 
                                           Select the table to activate and click the button""",
                               width=800, height=20)
        self.run_button = Button(label='Launch tabs', button_type='success')
        self.tablist = []
        self.selected_tracker = [] # used to monitor if a tab has already been launched

    def information_div(self):
        txt = """
            <h3 style='color:red;'>Info:</h3>
            <ul style='margin-top:-10px;'>
            <li>
            Select the tab(s) you want activated
            </li>
            <li>
            The click the 'launch activity' button.
            </li>
            </ul>
        """
        return Div(text=txt,width=400,height=400)

    def get_selections(self):
        self.selected_tabs= [self.selection_checkboxes.labels[i]
                            for i in self.selection_checkboxes.active]
        return self.selected_tabs

    def notification_updater(self, text):
        text = '<h4  style="color:red">{}</h4>'.format(text)
        self.notification_div.text = text

    def update_selections(self):
        self.notification_updater("When this messages disappears, the tabs are available for use")
        self.selected_tabs = [self.selection_checkboxes.labels[i]
                              for i in self.selection_checkboxes.active]
        self.notification_updater(text="")

    @coroutine
    def run_tab(self):
        try:
            self.selection_checkboxes.on_change('active',lambda attr, old, new: self.update_selections)


            #callbacks

            # COMPOSE LAYOUT
            # put the controls in a single element
            controls = WidgetBox(self.selection_checkboxes,self.run_button)

            # create the dashboards
            grid = gridplot([
                [self.notification_div],
                [controls, self.information_div()],
            ])

            # Make a tab with the layout
            tab = Panel(child=grid, title='Tab Selection')
            return tab

        except Exception:
            logger.error('rendering err:',exc_info=True)
            return tab_error_flag('poolminer')


