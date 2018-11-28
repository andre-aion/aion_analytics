from os.path import dirname, join
from multiprocessing.pool import ThreadPool
from functools import partial
from concurrent.futures import ThreadPoolExecutor
import asyncio
from dask import compute

import pandas as pd
import dask.dataframe as dd

# os methods for manipulating paths

# Bokeh basics
from bokeh.io import curdoc
from bokeh.models.widgets import Tabs, Div
from bokeh.server.server import Server
from bokeh.models import Panel


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
executor = ThreadPoolExecutor(max_workers=20)

kcp_connection = KafkaConnectPyspark()
kcp_connection.set_ssc(ssc)
executor.submit(kcp_connection.run())


def aion_analytics(doc):

    # SETUP BOKEH OBJECTS
    pm = Panel(child=Div(), title='Poolminer')
    tabs = Tabs(tabs=[pm])
    # Put the tabs in the current document for display
    doc.add_root(tabs)

    ###
    # Setup callbacks
    ###
    def current_state_block_df(kcp_state):
        return kcp_state.get_df()

    def launch_poolminer_tab(block_df):
        print("LAUNCHED FROM MAIN:{}".format(block_df.head()))
        return poolminer_tab(current_state_block_df(block_df))

    def no_blocks():
        # Make a tab with the layout
        div = Div(text=""" THERE IS NO BLOCK DATA AT PRESENT """,
        width=200, height=100)

        pm = Panel(child=div, title='Poolminer')
        return pm

    @coroutine
    @without_document_lock
    def update_poolminer_tab():
        kcp_stream = KafkaConnectPyspark()
        #  ----------------   TABS -----------------------------
        # Create
        block_df = yield executor.submit(current_state_block_df, kcp_stream)
        if not block_df:
            pm = yield executor.submit(no_blocks)
        else:
            pm = yield executor.submit(launch_poolminer_tab, block_df)
        doc.add_next_tick_callback(pm)
    # -----------------  START DATALOAD THREADS  --------------------

    pm.select_one(update_poolminer_tab)
    doc.add_next_tick_callback(tabs)



# Setting num_procs here means we can't touch the IOLoop before now, we must
# let Server handle that. If you need to explicitly handle IOLoops then you
# will need to use the lower level BaseServer class.
server = Server({'/': aion_analytics}, num_procs=2, port=0)
server.start()

if __name__ == '__main__':
    print('Opening Bokeh application on http://localhost:5006/')

    server.io_loop.add_callback(server.show, "/")
    server.io_loop.start()