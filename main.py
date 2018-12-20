from os.path import dirname, join
from concurrent.futures import ThreadPoolExecutor
import threading
from multiprocessing import Process


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

logger = mylogger(__file__)
executor = ThreadPoolExecutor(max_workers=10)


# set up stream runners
conf = SparkConf() \
    .set("spark.streaming.kafka.backpressure.initialRate", 150) \
    .set("spark.streaming.kafka.backpressure.enabled", 'true') \
    .set('spark.streaming.kafka.maxRatePerPartition', 250) \
    .set('spark.streaming.receiver.writeAheadLog.enable', 'true') \
    .set("spark.streaming.concurrentJobs", 2)
spark_context = SparkContext(appName='aion_analytics',conf=conf)
ssc = StreamingContext(spark_context,1)

def tx_runner(spark_context,conf,ssc):
    kcp_tx = KafkaConnectPyspark('transaction',spark_context,conf,ssc)
    kcp_tx.run()


def block_runner(spark_context,conf,ssc):
    kcp_block = KafkaConnectPyspark('block',spark_context,conf,ssc)
    kcp_block.run()

t1 = threading.Thread(target=tx_runner,args=(spark_context,conf,ssc,))
t1.daemon=True
t2 = threading.Thread(target=block_runner,args=(spark_context,conf,ssc,))
t2.daemon=True

@gen.coroutine
def aion_analytics(doc):
    ###
    # Setup callback
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
    #server = yield executor.submit(Server({'/': aion_analytics}, num_procs=1, port=0))
    server = Server({'/': aion_analytics}, num_procs=1, port=0)
    server.start()
    server.io_loop.add_callback(server.show, "/")
    server.io_loop.start()


if __name__ == '__main__':
    print('Opening Bokeh application on http://localhost:5006/')
    t1.start()
    t2.start()
    try:
        threading.Thread(target=launch_server).start()

    except Exception:
        logger.error("WEBSERVER LAUNCH:", exc_info=True)

