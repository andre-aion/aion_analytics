from concurrent.futures import ThreadPoolExecutor

from tornado import gen
from bokeh.document import without_document_lock

# Bokeh basics
from bokeh.models.widgets import Tabs
from bokeh.server.server import Server
from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler


# GET THE DASHBOARDS
from tornado.ioloop import IOLoop

from scripts.dashboards.blockminer import blockminer_tab
from scripts.dashboards.poolminer import poolminer_tab
from scripts.dashboards.churn.churn import churn_tab
from scripts.dashboards.churn.miner_predictive import miner_churn_predictive_tab
from scripts.dashboards.churn.network_predictive import network_churn_predictive_tab

from scripts.utils.mylogger import mylogger

logger = mylogger(__file__)
executor = ThreadPoolExecutor(max_workers=10)

@gen.coroutine
def aion_analytics(doc):

    # SETUP BOKEH OBJECTS
    try:
        #ch = yield churn_tab()
        #bm = yield blockminer_tab()
        #hr = yield hashrate_tab()
        #pm = yield poolminer_tab()
        ch_1 = yield miner_churn_predictive_tab(1)
        #ch_2 = yield miner_churn_predictive_tab(2)
        net_ch = yield network_churn_predictive_tab()

        tabs = Tabs(tabs=[net_ch,ch_1])
        doc.add_root(tabs)
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