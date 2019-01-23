from concurrent.futures import ThreadPoolExecutor

from bokeh.models import WidgetBox
from tornado import gen
from bokeh.document import without_document_lock

# Bokeh basics
from bokeh.models.widgets import Tabs, CheckboxGroup, Button, Panel, Div
from bokeh.server.server import Server
from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from bokeh.layouts import row, gridplot

# GET THE DASHBOARDS
from tornado.ioloop import IOLoop

from scripts.dashboards.blockminer import blockminer_tab
from scripts.dashboards.poolminer import poolminer_tab
from scripts.dashboards.churn.churn import churn_tab
from scripts.dashboards.churn.tier1_miner_predictive import tier1_miner_churn_predictive_tab
from scripts.dashboards.churn.tier2_miner_predictive import tier2_miner_churn_predictive_tab
from scripts.dashboards.churn.network_predictive import network_activity_predictive_tab
from scripts.utils.mylogger import mylogger

logger = mylogger(__file__)
executor = ThreadPoolExecutor(max_workers=10)


labels = [
    'blockminer_tab',
    'poolminer_tab',
    'churn_tab',
    'tier1_miner_churn_predictive_tab',
    'tier2_miner_churn_predictive_tab',
    'network_activity_predictive_tab']


class SelectionTab:
    def __init__(self):

        self.selected_tabs = []
        self.tablist = []
        self.selected_tracker = [] # used to monitor if a tab has already been launched

    def get_selections(self,checkboxes):
        self.selected_tabs= [checkboxes.labels[i] for i in checkboxes.active]
        return self.selected_tabs



selection_tab = SelectionTab()

@gen.coroutine
def aion_analytics(doc):
    # SETUP BOKEH OBJECTS
    try:
        selection_tab.tablist = []
        TABS = Tabs(tabs=selection_tab.tablist)
        @gen.coroutine
        def load_callstack():
            lst = selection_tab.get_selections(selection_checkboxes)
            logger.warning('selections:%s',lst)
            '''
            if 'blockminer_tab' in lst:
                bm = yield blockminer_tab()
                tablist.append(bm)
            '''

            if 'poolminer_tab' in lst:
                if 'pooliminer_tab' not in selection_tab.selected_tracker:
                    pm = yield poolminer_tab()
                    selection_tab.selected_tracker.append('poolminer_tab')
                    if pm not in selection_tab.tablist:
                        selection_tab.tablist.append(pm)

            if 'churn_tab' in lst:
                if 'churn_tab' not in selection_tab.selected_tracker:
                    ch = yield churn_tab()
                    selection_tab.selected_tracker.append('churn_tab')
                    if ch not in selection_tab.tablist:
                        selection_tab.tablist.append(ch)

            if 'tier1_miner_churn_predictive_tab' in lst:
                if 'tier1_miner_churn_predictive_tab' not in selection_tab.selected_tracker:
                    mch_1 = yield tier1_miner_churn_predictive_tab()
                    selection_tab.selected_tracker.append('tier1_miner_churn_predictive_tab')
                    if mch_1 not in selection_tab.tablist:
                        selection_tab.tablist.append(mch_1)

            if 'tier2_miner_churn_predictive_tab' in lst:
                if 'tier2_miner_churn_predictive_tab' not in selection_tab.selected_tracker:
                    mch_2 = yield tier2_miner_churn_predictive_tab()
                    selection_tab.selected_tracker.append('tier2_miner_churn_predictive_tab')
                    if mch_2 not in selection_tab.tablist:
                        selection_tab.tablist.append(mch_2)

            if 'network_activity_predictive_tab' in lst:
                if 'network_activity_predictive_tab' not in selection_tab.selected_tracker:
                    nap = yield network_activity_predictive_tab()
                    selection_tab.selected_tracker.append('network_activity_predictive_tab')
                    if nap not in selection_tab.tablist:
                        selection_tab.tablist.append(nap)

            #make list unique
            selection_tab.tablist = list(set(selection_tab.tablist))
            TABS.update(tabs=selection_tab.tablist)

        @gen.coroutine
        def select_tabs():
            notification_div.text ="When this messages disappears, the tab(s) are available for use"
            yield load_callstack()
            notification_div.text =""

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
        information_div = Div(text=txt, width=400, height=400)
        notification_div = Div(text='',width=700,height=20)
        selection_checkboxes = CheckboxGroup(labels=labels, active=[0])
        run_button = Button(label='Launch tabs', button_type="success")
        run_button.on_click(select_tabs)

        controls = WidgetBox(selection_checkboxes, run_button)

        # create the dashboards
        grid = gridplot([
            [notification_div],
            [controls, information_div],
        ])
        mgmt = Panel(child=grid, title='Tab Selection')

        pm = yield blockminer_tab()
        selection_tab.tablist.append(mgmt)
        selection_tab.tablist.append(pm)
        selection_tab.selected_tracker.append('blockminer_tab')
        TABS.update(tabs=selection_tab.tablist)
        #tabs.on_change()
        doc.add_root(TABS)
    except Exception:
        logger.error("TABS:", exc_info=True)

# configure and run bokeh server
@gen.coroutine
@without_document_lock
def launch_server():
    try:
        apps = {"/aion-analytics": Application(FunctionHandler(aion_analytics))}
        io_loop = IOLoop.current()
        server = Server(apps,port=5006,allow_websocket_origin=["*"],io_loop=io_loop,
                        session_ids='external-signed')
        server.start()
        server.io_loop.add_callback(server.show, '/aion-analytics')
        server.io_loop.start()
    except Exception:
        logger.error("WEBSERVER LAUNCH:", exc_info=True)

if __name__ == '__main__':
    launch_server()