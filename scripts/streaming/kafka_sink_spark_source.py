import os
import sys
module_path = os.path.abspath(os.getcwd() + '\\..')
if module_path not in sys.path:
    sys.path.append(module_path)

from scripts.utils.pythonCassandra import PythonCassandra
from scripts.utils import myutils
from scripts.streaming.streamingBlock import Block
from tornado.gen import coroutine
from tornado.locks import Condition, Semaphore
from bokeh.document import without_document_lock
from concurrent.futures import ThreadPoolExecutor

import json
import datetime

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from queue import Queue
from dask.distributed import Client
import gc
from copy import copy
import pandas as pd
import dask as dd

import config
from scripts.utils.mylogger import mylogger
logger = mylogger(__file__)


executor = ThreadPoolExecutor(max_workers=10)
condition = Condition()
sem = Semaphore
class KafkaConnectPyspark:
    block_columns = [
        "block_number", "miner_address", "miner_addr"
        "nonce", "difficulty",
        "total_difficulty", "nrg_consumed", "nrg_limit",
        "size", "block_timestamp", 'block_month', "num_transactions",
        "block_time", "nrg_reward", "transaction_id", "transaction_list"]
    block = Block()

    def __init__(self):

        '''
        # cassandra setup
        self.pc = PythonCassandra()
        self.pc.setlogger()
        self.pc.createsession()
        self.pc.createkeyspace('aionv4')
        '''

        '''
        self.client = Client('127.0.0.1:8786')
        self.input_queue = Queue()
        self.remote_queue = self.client.scatter(self.input_queue)
        '''

    @classmethod
    def update_block_df(self, messages):
        #print(self.block.df.get_df().head())
        # convert to dataframe using stream

        #source = source.map(messages).map(df.add_data)
        #source.sink(self.block_df.add_data)
        try:
            #self.block.add_data(messages)
            #self.copy()
            config.block.add_data(messages)
            print('&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&')
            print('################################################################')
            #print('STREAMING DATAFRAME UPDATED: TAIL PRINTED BELOW')
            #print(self.block.df.get_df().tail())
            #print(config.block.get_df().tail())
        except Exception:
            logger.error("ERROR IN UPDATE BLOCK WITH MESSAGES:", exc_info=True)


    @classmethod
    def set_ssc(self, ssc):
        if 'self.ssc' not in locals():
            self.ssc = ssc

    # return column data source
    @classmethod
    def get_block_source(self):
        return self.block_source

    @classmethod
    def get_df(self):
        return self.block.get_df()

    @classmethod
    def get_queue(self):
        return self.remote_queue.get()

    @classmethod
    def update_cassandra(self,message):
        self.pc.insert_data_block([message])

    @classmethod
    def block_to_tuple(self, taken):
        messages = list()
        message_dask = {}
        counter = 1

        for mess in taken:
            #print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            #print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            block_month = myutils.get_month_from_timestamp(mess['block_timestamp'])
            message = (mess["block_number"], mess["block_hash"], mess["miner_address"],
                       mess["parent_hash"], mess["receipt_tx_root"],
                       mess["state_root"], mess["tx_trie_root"], mess["extra_data"],
                       mess["nonce"], mess["bloom"], mess["solution"], mess["difficulty"],
                       mess["total_difficulty"], mess["nrg_consumed"], mess["nrg_limit"],
                       mess["size"], mess["block_timestamp"], block_month, mess["num_transactions"],
                       mess["block_time"], mess["nrg_reward"], mess["transaction_id"], mess["transaction_list"])
            #print(message)
            print('message counter in taken:{}'.format(counter))
            #print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            #print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")



            # munge data
            # convert timestamp, add miner_addr
            @coroutine
            def munge_data():
                for col in config.block.columns:
                    if col in mess:
                        if col == 'block_timestamp':  # get time columns
                            block_timestamp = datetime.datetime.fromtimestamp(mess[col]) \
                                .strftime('%Y-%m-%d %H:%M:%S')
                            if col not in message_dask:
                                message_dask[col] = []
                            message_dask[col].append(block_timestamp)

                            block_month = myutils.get_month_from_timestamp(mess[col])
                            if 'block_month' not in message_dask:
                                message_dask['block_month'] = []
                            message_dask['block_month'].append(block_month)

                        elif col == 'miner_address': # truncate miner address
                            if col not in message_dask:
                                message_dask[col] = []
                            message_dask[col].append(mess[col])

                            if 'miner_addr' not in message_dask:
                                message_dask['miner_addr'] = []
                            message_dask['miner_addr'].append(mess[col][0:10])

                        # convert difficulty
                        elif col == 'difficulty':
                            if col not in message_dask:
                                message_dask[col] = []
                            message_dask[col].append(int(mess[col], base=16))

                        else:
                            if col not in message_dask:
                                message_dask[col] = []
                            message_dask[col].append(mess[col])

            munge_data()

            # regulate # messages in one dict
            if counter >= 26:
                #  update streaming dataframe
                self.update_block_df(message_dask)
                print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                print('message counter:{}'.format(counter))
                #print(message_dask)
                print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                counter = 1
                message_dask = {}

            else:
                counter += 1
                #messages.append(message_dask)
                #self.pc.update_cassandra(message)

                del mess
                gc.collect()

        self.update_block_df(message_dask) # get remainder messages in last block thats less than 501
        del messages
        del message_dask


    @classmethod
    def handle_rdds(self,rdd):
        if rdd.isEmpty():
            print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            print('################  RDD IS NONE')
            print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            return

        try:
            taken = rdd.take(20000)
            self.block_to_tuple(taken)
        except Exception:
            logger.error('HANDLE RDDS:{}',exc_info=True)

    @classmethod
    @coroutine
    def copy(self):
        logger.warning('about to copy')
        config.block = copy(self.block)
        condition.notify()
        logger.warning('done copying')


    @classmethod
    def run(self):
        topic='mainnetserver.aionv4.block'
        kafkaParams = {"metadata.broker.list": "localhost:9092",
                       "auto.offset.reset": "smallest"}

        try:
            kafkaStream = KafkaUtils.createDirectStream(self.ssc,
                                                             [topic], kafkaParams)
            kafkaStream.checkpoint(30)

            kafkaStream = kafkaStream.map(lambda x: json.loads(x[1]))
            kafkaStream = kafkaStream.map(lambda x: x['payload']['after'])
            #kafkaStream.pprint()

            kafkaStream.foreachRDD(lambda rdd: self.handle_rdds(rdd) if not rdd.isEmpty() else None)

            self.ssc.start()
            self.ssc.awaitTermination()

        except Exception as ex:
            print('THE SPARK SOURCE IS EMPTY:{}'.format(ex))




