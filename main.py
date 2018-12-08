from os.path import dirname, join
from concurrent.futures import ThreadPoolExecutor
import asyncio
from tornado import gen
from tornado.locks import Condition
from bokeh.document import without_document_lock


# Bokeh basics
from bokeh.io import curdoc
from bokeh.models.widgets import Tabs, Div
from bokeh.server.server import Server
from bokeh.io import curdoc


from pdb import set_trace

# IMPORT HELPERS
import config
from scripts.streaming.kafka_sink_spark_source import KafkaConnectPyspark

# GET THE DASHBOARDS
from scripts.dashboards.streaming.poolminer import poolminer_tab
from scripts.dashboards.streaming.hashrate import hashrate_tab
from scripts.utils.mylogger import mylogger


from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.context import SparkConf, SparkContext


executor = ThreadPoolExecutor(max_workers=20)
kcp_connection = KafkaConnectPyspark()
logger = mylogger(__file__)


@gen.coroutine
def block_update():
    global kcp_connection
    kcp_connection.run()


# run block streamer
executor.submit(block_update)
#block_update()

@gen.coroutine
def aion_analytics(doc):
    ###
    # Setup callbacks
    ###

    # SETUP BOKEH OBJECTS
    try:
        pm = yield poolminer_tab()
        hr = yield hashrate_tab()

        tabs = Tabs(tabs=[pm, hr])
        doc.add_root(tabs)

    except Exception:
        logger.error("TABS:", exc_info=True)

# Setting num_procs here means we can't touch the IOLoop before now, we must
# let Server handle that. If you need to explicitly handle IOLoops then you
# will need to use the lower level BaseServer class.
@gen.coroutine
@without_document_lock
def launch_server():
    #server = yield executor.submit(Server({'/': aion_analytics}, num_procs=3, port=0))
    server = Server({'/': aion_analytics}, num_procs=1, port=0)
    server.start()
    server.io_loop.add_callback(server.show, "/")
    server.io_loop.start()


if __name__ == '__main__':
    print('Opening Bokeh application on http://localhost:5006/')
    try:
        launch_server()
    except Exception:
        logger.error("WEBSERVER LAUNCH:", exc_info=True)