from concurrent.futures import ThreadPoolExecutor

from tornado import gen
from bokeh.document import without_document_lock

# Bokeh basics
from bokeh.models.widgets import Tabs
from bokeh.server.server import Server

# GET THE DASHBOARDS
from scripts.dashboards.blockminer import blockminer_tab
from scripts.dashboards.hashrate import hashrate_tab
from scripts.dashboards.poolminer import poolminer_tab
from scripts.utils.mylogger import mylogger

logger = mylogger(__file__)
executor = ThreadPoolExecutor(max_workers=10)

@gen.coroutine
def aion_analytics(doc):

    # SETUP BOKEH OBJECTS
    try:
        bm = yield blockminer_tab()
        #hr = yield hashrate_tab()
        pm = yield poolminer_tab()
        tabs = Tabs(tabs=[pm, bm])
        doc.add_root(tabs)

    except Exception:
        logger.error("TABS:", exc_info=True)


# Setting num_procs here means we can't touch the IOLoop before now, we must
# let Server handle that. If you need to explicitly handle IOLoops then you
# will need to use the lower level BaseServer class.
@gen.coroutine
@without_document_lock
def launch_server():
    try:
        server = Server({'/': aion_analytics}, num_procs=1, port=5006)
        server.start()
        server.io_loop.add_callback(server.show, "/")
        server.io_loop.start()

    except Exception:
        logger.error("WEBSERVER LAUNCH:", exc_info=True)


if __name__ == '__main__':
    print('Opening Bokeh application on http://localhost:5006/')
    launch_server()



