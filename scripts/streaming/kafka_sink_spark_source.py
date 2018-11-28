import os
import sys
module_path = os.path.abspath(os.getcwd() + '\\..')
if module_path not in sys.path:
    sys.path.append(module_path)


from scripts.utils.pythonCassandra import PythonCassandra
from scripts.utils import myutils
from scripts.streaming.streamingDataframe import *
from scripts.streaming.streamingBlock import Block
from tornado.gen import coroutine
from bokeh.document import without_document_lock
from concurrent.futures import ThreadPoolExecutor

import json
import datetime

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from streamz import Stream
from queue import Queue
from dask.distributed import Client

executor = ThreadPoolExecutor(max_workers=10)

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



        self.block_stream = Stream()
        '''
        self.client = Client('127.0.0.1:8786')
        self.input_queue = Queue()
        self.remote_queue = self.client.scatter(self.input_queue)
        '''

    @classmethod
    @coroutine
    @without_document_lock
    def update_block_df(self, messages):
        print(self.block.df.get_df().head())
        # convert to dataframe using stream
        #df = Block()
        #source = Stream()
        #source = source.map(messages).map(df.add_data)
        #source.sink(self.block_df.add_data)
        try:
            self.block.df.add_data(messages)
            print('&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&')
            print('################################################################')
            print('STREAMING DATAFRAME UPDATED: TAIL PRINTED BELOW')
            print(self.block.df.get_df().tail())
        except Exception as ex:
            print("ERROR IN UPDATE BLOCK WITH MESSAGES:{}".format(ex))

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
        return self.block_df

    @classmethod
    def get_queue(self):
        return self.remote_queue.get()

    @classmethod
    def update_cassandra(self,message):
        self.pc.insert_data_block([message])

    @classmethod
    @coroutine
    def block_to_tuple(self, taken):
        messages = list()
        message1 = {}
        counter = 1

        for mess in taken:
            print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            block_month = myutils.get_month_from_timestamp(mess['block_timestamp'])
            message = (mess["block_number"], mess["block_hash"], mess["miner_address"],
                       mess["parent_hash"], mess["receipt_tx_root"],
                       mess["state_root"], mess["tx_trie_root"], mess["extra_data"],
                       mess["nonce"], mess["bloom"], mess["solution"], mess["difficulty"],
                       mess["total_difficulty"], mess["nrg_consumed"], mess["nrg_limit"],
                       mess["size"], mess["block_timestamp"], block_month, mess["num_transactions"],
                       mess["block_time"], mess["nrg_reward"], mess["transaction_id"], mess["transaction_list"])
            #print(message)
            print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")



            # munge data
            # convert timestamp, add miner_addr
            for col in self.block.columns:
                if col in mess:
                    if col == 'block_timestamp':
                        block_timestamp = datetime.datetime.fromtimestamp(mess['block_timestamp']) \
                            .strftime('%Y-%m-%d %H:%M:%S')
                        if col not in message1:
                            message1[col] = []
                        message1[col].append(block_timestamp)

                        block_month = myutils.get_month_from_timestamp(mess['block_timestamp'])
                        if 'block_month' not in message1:
                            message1['block_month'] = []
                        message1['block_month'].append(block_month)

                    elif col == 'miner_address':
                        if col not in message1:
                            message1[col] = []
                        message1[col].append(mess[col])
                        if 'miner_addr' not in message1:
                            message1['miner_addr'] = []
                        message1['miner_addr'].append(mess[col][0:10])
                    else:
                        if col not in message1:
                            message1[col] = []
                        message1[col].append(mess[col])

            # control message count
            while counter < 501:
                counter += 1
            if counter >= 501:
                counter = 1
                #  update streaming dataframe
                self.update_block_df(message1)
                print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                print(message1)
                print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                message1 = {}


            #messages.append(message1)
            #self.pc.update_cassandra(message)

            del mess
            gc.collect()

        del messages
        del message1


    @classmethod
    @coroutine
    def handle_rdds(self,rdd):
        if rdd.isEmpty():
            print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            print('################  RDD IS NONE')
            print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            return

        try:
            taken = rdd.take(20000)
            self.block_to_tuple(taken)
        except Exception as ex:
            print('HANDLE RDDS:{}'.format(ex))


    @classmethod
    @coroutine
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




