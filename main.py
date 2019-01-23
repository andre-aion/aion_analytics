from concurrent.futures import ThreadPoolExecutor

from tornado import gen
from bokeh.document import without_document_lock

# Bokeh basics
from bokeh.models.widgets import Tabs, CheckboxGroup
from bokeh.server.server import Server
from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from bokeh.layouts import row


# GET THE DASHBOARDS
from tornado.ioloop import IOLoop

from scripts.dashboards.blockminer import blockminer_tab
from scripts.dashboards.poolminer import poolminer_tab
from scripts.dashboards.churn.churn import churn_tab
from scripts.dashboards.churn.tier1_miner_predictive import tier1_miner_churn_predictive_tab
from scripts.dashboards.churn.tier2_miner_predictive import tier2_miner_churn_predictive_tab
from scripts.dashboards.churn.network_predictive import network_activity_predictive_tab
from scripts.dashboards.selection_tab import SelectionTab
from scripts.utils.mylogger import mylogger

logger = mylogger(__file__)
executor = ThreadPoolExecutor(max_workers=10)
selection_tab = SelectionTab()

@gen.coroutine
def aion_analytics(doc):
    # SETUP BOKEH OBJECTS
    try:
        selection_tab.tablist = []
        TABS = Tabs(tabs=selection_tab.tablist)
        @gen.coroutine
        def load_callstack():
            lst = selection_tab.get_selections()
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
            selection_tab.notification_updater("When this messages disappears, the tab(s) are available for use")
            yield load_callstack()
            selection_tab.notification_updater(text="")

        # callback
        selection_tab.run_button.on_click(select_tabs)

        mgmt = yield selection_tab.run_tab()
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
                        session_ids='external signed')
        server.start()
        server.io_loop.add_callback(server.show, '/aion-analytics')
        server.io_loop.start()
    except Exception:
        logger.error("WEBSERVER LAUNCH:", exc_info=True)

if __name__ == '__main__':
    launch_server()