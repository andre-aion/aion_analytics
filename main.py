from os.path import dirname, join
from multiprocessing.pool import ThreadPool
from functools import partial
from concurrent.futures import ThreadPoolExecutor
import asyncio
from dask import compute
from tornado import gen

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

@gen.coroutine
def block_update():
    kcp_connection = KafkaConnectPyspark()
    global kcp_connection
    kcp_connection.set_ssc(ssc)
    kcp_connection.run()


executor.submit(block_update)


@gen.coroutine
@without_document_lock
def aion_analytics(doc):
    ###
    # Setup callbacks
    ###

    def current_state_block_df():
        return kcp_connection.get_df()

    def launch_poolminer_tab(block_df):
        print("LAUNCHED FROM MAIN:{}".format(block_df.head()))
        yield poolminer_tab(current_state_block_df(block_df))

    def launch_hashrate_tab(block_df):
        print("LAUNCHED FROM MAIN:{}".format(block_df.head()))
        yield poolminer_tab(current_state_block_df(block_df))


    def no_data(title):
        # Make a tab with the layout
        text = """ THERE IS NO {} DATA AT PRESENT""".format(title)
        div = Div(text=text,width=200, height=100)

        pm = Panel(child=div, title=title)
        return pm

    def update_poolminer_tab():
        #  ----------------   TABS -----------------------------
        # Create
        block_df = current_state_block_df()
        if len(block_df) <= 0:
            return no_data('poolminer')
        else:
            return launch_poolminer_tab(block_df)

    def update_hashrate_tab():
        #  ----------------   TABS -----------------------------
        # Create
        block_df = current_state_block_df()
        if len(block_df) <= 0:
            return no_data('hashrate')
        else:
            return launch_poolminer_tab(block_df)


    # SETUP BOKEH OBJECTS
    pm = update_poolminer_tab()
    hr = update_hashrate_tab()

    tabs = Tabs(tabs=[pm, hr])

    doc.add_root(tabs)

    # -----------------  START DATALOAD THREADS  --------------------
    #doc.add_next_tick_callback(block_update)


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
    except Exception as ex:
        print("WEBSERVER MAIN EXCEPTION:{}".format(ex))