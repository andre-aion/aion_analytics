from concurrent.futures import ThreadPoolExecutor

from bokeh.io import curdoc
from tornado import gen
from bokeh.document import without_document_lock

# Bokeh basics
from bokeh.models.widgets import Tabs
from bokeh.server.server import Server

# GET THE DASHBOARDS
from scripts.dashboards.blockminer import blockminer_tab
from scripts.dashboards.churn import churn_tab
from scripts.dashboards.hashrate import hashrate_tab
from scripts.dashboards.poolminer import poolminer_tab
from scripts.utils.mylogger import mylogger

logger = mylogger(__file__)
executor = ThreadPoolExecutor(max_workers=10)

@gen.coroutine
def aion_analytics(doc):

    # SETUP BOKEH OBJECTS
    try:
        #ch = yield churn_tab()
        bm = yield blockminer_tab()
        #hr = yield hashrate_tab()
        #pm = yield poolminer_tab()

        tabs = Tabs(tabs=[bm])
        doc.add_root(tabs)

    except Exception:
        logger.error("TABS:", exc_info=True)

doc = curdoc()
aion_analytics(doc)
