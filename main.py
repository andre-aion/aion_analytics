from os.path import dirname, join
from multiprocessing.pool import ThreadPool
from functools import partial
from concurrent.futures import ThreadPoolExecutor
from dask import compute

import pandas as pd
import dask.dataframe as dd

# os methods for manipulating paths

# Bokeh basics
from bokeh.io import curdoc
from bokeh.models.widgets import Tabs
from bokeh.server.server import Server


from pdb import set_trace

# IMPORT HELPERS
# from scripts.sentiment import sentiment_dashboard
from scripts.utils.hashrate import calc_hashrate
from scripts.utils.myutils import convert_block_timestamp_from_string, setdatetimeindex
from scripts.utils.pythonCassandra import PythonCassandra
from scripts.streaming.streamingBlock import *
from scripts.streaming.kafka_sink_spark_source import *

# GET THE DASHBOARDS
from scripts.dashboards.streaming.poolminer import poolminer_tab
from scripts.dashboards.hashrate import hashrate_tab

from dask.distributed import Client
from streamz import Stream

from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.context import SparkConf,SparkContext

conf = SparkConf() \
    .set("spark.streaming.kafka.backpressure.initialRate", 500)\
    .set("spark.streaming.kafka.backpressure.enabled",'true')\
    .set('spark.streaming.kafka.maxRatePerPartition',10000)

sparkContext = SparkContext().getOrCreate()
spark = SparkSession \
    .builder \
    .appName('poolminer') \
    .master('local[2]') \
    .config(conf=conf) \
    .getOrCreate()

ssc = StreamingContext(sparkContext, 0.5)  #
ssc.checkpoint('data/sparkcheckpoint')
sqlContext = SQLContext(sparkContext)

#doc = curdoc()
executor = ThreadPoolExecutor(max_workers=10)

kcp = KafkaConnectPyspark()
kcp.set_ssc(ssc)
kcp.run()


def modify_doc(doc):
    # -----------------  START DATALOAD THREADS  --------------------

    kcp1 = KafkaConnectPyspark()
    #  ----------------   TABS -----------------------------
    # Create
    pm = poolminer_tab(kcp1.block_df)
    # Put all the tabs into one application
    tabs = Tabs(tabs=[pm])
    #doc.add_next_tick_callback(tabs)

    # Put the tabs in the current document for display
    doc.add_root(tabs)


# Setting num_procs here means we can't touch the IOLoop before now, we must
# let Server handle that. If you need to explicitly handle IOLoops then you
# will need to use the lower level BaseServer class.
server = Server({'/': modify_doc}, num_procs=1, port=0)
server.start()

if __name__ == '__main__':
    print('Opening Bokeh application on http://localhost:5006/')

    server.io_loop.add_callback(server.show, "/")
    server.io_loop.start()