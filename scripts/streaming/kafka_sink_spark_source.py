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
        "block_number", "miner_address",
        "nonce", "difficulty",
        "total_difficulty", "nrg_consumed", "nrg_limit",
        "size", "block_timestamp", "num_transactions",
        "block_time", "nrg_reward", "transaction_id", "transaction_list"]
    block = Block()
    # cassandra setup
    pc = PythonCassandra()
    pc.setlogger()
    pc.createsession()
    pc.createkeyspace('aionv4')
    pc.create_table_block()

    def __init__(self):
        '''
        self.client = Client('127.0.0.1:8786')
        self.input_queue = Queue()
        self.remote_queue = self.client.scatter(self.input_queue)
        '''

    @classmethod
    @coroutine
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
    @coroutine
    def update_cassandra(self, messages):
        self.pc.insert_data_block(messages)

    @classmethod
    def block_to_tuple(self, taken):
        messages_cass = list()
        message_dask = {}
        counter = 1

        for mess in taken:
            #print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            #print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            #print(message)
            #print('message counter in taken:{}'.format(counter))
            print('blocks loaded from taken:{}'.format(mess['block_number']))

            config.max_block_loaded = mess["block_number"]
            #print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            #print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")


            # munge data
            # convert timestamp, add miner_addr
            #@coroutine
            def munge_data():
                message_temp = {}
                for col in config.block.columns:
                    if col in mess:
                        if col == 'block_timestamp':  # get time columns
                            block_timestamp = datetime.datetime.fromtimestamp(mess[col])
                            if col not in message_dask:
                                message_dask[col] = []
                            message_dask[col].append(block_timestamp)
                            message_temp[col] = block_timestamp
                            block_month, block_date = myutils.get_breakdown_from_timestamp(mess[col])
                            if 'block_date' not in message_dask:
                                message_dask['block_date'] = []
                            message_dask['block_date'].append(block_date)
                            message_temp['block_date'] = block_date

                            if 'block_month' not in message_dask:
                                message_dask['block_month'] = []
                            message_dask['block_month'].append(block_month)
                            message_temp['block_month'] = block_month

                        elif col == 'miner_address': # truncate miner address
                            if col not in message_dask:
                                message_dask[col] = []
                            message_dask[col].append(mess[col])
                            message_temp[col] = mess[col]

                            if 'miner_addr' not in message_dask:
                                message_dask['miner_addr'] = []
                            message_dask['miner_addr'].append(mess[col][0:10])
                            message_temp['miner_addr'] = mess[col][0:10]
                            
                        # convert difficulty
                        elif col == 'difficulty':
                            if col not in message_dask:
                                message_dask[col] = []
                            message_dask[col].append(int(mess[col], base=16))
                            message_temp[col] = (int(mess[col], base=16))
                        else:
                            if col not in message_dask:
                                message_dask[col] = []
                            message_dask[col].append(mess[col])
                            message_temp[col] = mess[col]

                message = (message_temp["block_number"], message_temp["miner_address"],
                           message_temp["miner_addr"],message_temp["nonce"], message_temp["difficulty"],
                           message_temp["total_difficulty"], message_temp["nrg_consumed"], message_temp["nrg_limit"],
                           message_temp["size"], message_temp["block_timestamp"],
                           message_temp["block_date"], message_temp["block_month"],
                           message_temp["num_transactions"],
                           message_temp["block_time"], message_temp["nrg_reward"], message_temp["transaction_id"],
                           message_temp["transaction_list"])

                return message
                # insert to cassandra
                # self.update_cassandra(message)
            message_cass = munge_data()
            messages_cass.append(message_cass)

            # regulate # messages in one dict
            if counter >= 10:
                #  update streaming dataframe
                self.update_block_df(message_dask)
                self.update_cassandra(messages_cass)
                messages_cass = list()
                print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                print('message counter:{}'.format(counter))
                print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                counter = 1
                message_dask = {}
            else:
                counter += 1
                del mess
                gc.collect()

        self.update_block_df(message_dask) # get remainder messages in last block thats less than 501
        self.update_cassandra(messages_cass)
        del messages_cass
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




