import os
import sys

from numpy import long

module_path = os.path.abspath(os.getcwd() + '\\..')
if module_path not in sys.path:
    sys.path.append(module_path)

from scripts.utils.pythonCassandra import PythonCassandra
from scripts.utils import myutils
from scripts.streaming.streamingBlock import Block
from tornado.gen import coroutine
from tornado.locks import Condition, Semaphore
from concurrent.futures import ThreadPoolExecutor

import json
import datetime

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.context import SparkConf, SparkContext

import gc

import config
from scripts.utils.mylogger import mylogger
logger = mylogger(__file__)


executor = ThreadPoolExecutor(max_workers=10)
CHECKPOINT_DIR = '/data/sparkcheckpoint'
ZOOKEEPER_SERVERS = "127.0.0.1:2181"
ZK_CHECKPOINT_PATH = '/opt/zookeeper/aion_analytics/offsets/'
ZK_CHECKPOINT_PATH = 'consumers/'


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
    checkpoint_dir = CHECKPOINT_DIR
    zk_checkpoint_dir = ZK_CHECKPOINT_PATH


    def __init__(self):
        '''
        cls.client = Client('127.0.0.1:8786')
        cls.input_queue = Queue()
        cls.remote_queue = cls.client.scatter(cls.input_queue)
        '''


    @classmethod
    @coroutine
    def update_block_df(cls, messages):
        # convert to dataframe using stream
        try:
            #cls.block.add_data(messages)
            #cls.copy()
            #config.block.add_data(messages)
            print('&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&')
            print('################################################################')
            #print('STREAMING DATAFRAME UPDATED: TAIL PRINTED BELOW')
            #print(cls.block.df.get_df().tail())
            #print(config.block.get_df().tail())
        except Exception:
            logger.error("ERROR IN UPDATE BLOCK WITH MESSAGES:", exc_info=True)


    @classmethod
    def set_ssc(cls, ssc):
        if 'cls.ssc' not in locals():
            cls.ssc = ssc

    @classmethod
    def get_df(cls):
        return cls.block.get_df()

    @classmethod
    def get_queue(cls):
        return cls.remote_queue.get()

    @classmethod
    @coroutine
    def update_cassandra(cls, messages):
        cls.pc.insert_data_block(messages)

    @classmethod
    def block_to_tuple(cls, taken):
        messages_cass = list()
        message_dask = {}
        counter = 1

        for mess in taken:
            #print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            #print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            #print(message)
            #print('message counter in taken:{}'.format(counter))
            print('block # loaded from taken:{}'.format(mess['block_number']))

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
                # cls.update_cassandra(message)
            message_cass = munge_data()
            messages_cass.append(message_cass)

            # regulate # messages in one dict
            if counter >= 10:
                #  update streaming dataframe
                cls.update_block_df(message_dask)
                cls.update_cassandra(messages_cass)
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

        cls.update_block_df(message_dask) # get remainder messages in last block thats less than 501
        cls.update_cassandra(messages_cass)
        del messages_cass
        del message_dask


    @classmethod
    def handle_rdds(cls,rdd):
        if rdd.isEmpty():
            print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            print('################  RDD IS NONE')
            print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            return
        try:
            taken = rdd.take(20000)
            cls.block_to_tuple(taken)
            #cls.save_offsets(rdd)

        except Exception:
            logger.error('HANDLE RDDS:{}',exc_info=True)

    @classmethod
    def get_zookeeper_instance(cls):
        from kazoo.client import KazooClient

        if 'KazooSingletonInstance' not in globals():
            globals()['KazooSingletonInstance'] = KazooClient(ZOOKEEPER_SERVERS)
            globals()['KazooSingletonInstance'].start()
        return globals()['KazooSingletonInstance']

    @classmethod
    def read_offsets(cls, zk, topics):
        try:
            from_offsets = {}
            for topic in topics:
                logger.warning("TOPIC:%s", topic)
                #create path if it does not exist
                topic_path = ZK_CHECKPOINT_PATH + topic

                try:
                    partitions = zk.get_children(topic_path)
                    for partition in partitions:
                        topic_partition = TopicAndPartition(topic, int(partition))
                        partition_path = topic_path + '/' + partition
                        offset = int(zk.get(partition_path)[0])
                        from_offsets[topic_partition] = offset
                except Exception:
                    try:
                        topic_partition = TopicAndPartition(topic, int(0))
                        zk.ensure_path(topic_path+'/'+"0")
                        zk.set(topic_path, str(0).encode())
                        from_offsets[topic_partition] = int(0)
                        logger.warning("NO OFFSETS")
                    except Exception:
                        logger.error('MAKE FIRST OFFSET:{}', exc_info=True)

            logger.warning("FROM_OFFSETS:%s",from_offsets)
            return from_offsets
        except Exception:
            logger.error('READ OFFSETS:{}',exc_info=True)

    @classmethod
    @coroutine
    def save_offsets(cls, rdd):
        print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        print("inside save offsets:%s")
        print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')

        try:
            zk = cls.get_zookeeper_instance()
            logger.warning("inside save offsets:%s", zk)
            for offset in rdd.offsetRanges():
                logger.warning("offset saved:%s",offset)
                path = ZK_CHECKPOINT_PATH + offset.topic + '/' + str(offset.partition)
                zk.ensure_path(path)
                zk.set(path, str(offset.untilOffset).encode())
        except Exception:
            logger.error('SAVE OFFSETS:%s',exc_info=True)

    @classmethod
    def create_streaming_context(cls):
        conf = SparkConf() \
            .set("spark.streaming.kafka.backpressure.initialRate", 500) \
            .set("spark.streaming.kafka.backpressure.enabled", 'true') \
            .set('spark.streaming.kafka.maxRatePerPartition', 10000) \
            .set('spark.streaming.receiver.writeAheadLog.enable', 'true')

        spark_context = SparkContext(appName='aion_analytics',
                                     conf=conf)

        ssc = StreamingContext(spark_context, 1)

        return ssc


    @classmethod
    def run(cls):
        try:
            # Get StreamingContext from checkpoint data or create a new one
            #cls.ssc = StreamingContext.getActiveOrCreate(cls.zk_checkpoint_dir,cls.create_streaming_context)
            cls.ssc = cls.create_streaming_context()

            # SETUP KAFKA SOURCE
            topics = ['mainnetserver.aionv4.block']
            # setup checkpointing
            zk = cls.get_zookeeper_instance()
            from_offsets = cls.read_offsets(zk, topics)

            kafka_params = {"metadata.broker.list": "localhost:9092",
                            "auto.offset.reset": "smallest"}


            kafka_stream = KafkaUtils \
                .createDirectStream(cls.ssc, topics, kafka_params,
                                    fromOffsets=from_offsets)

            kafka_stream.foreachRDD(lambda rdd: cls.save_offsets(rdd))
            #kafka_stream.checkpoint(30)

            kafka_stream = kafka_stream.map(lambda x: json.loads(x[1]))
            kafka_stream = kafka_stream.map(lambda x: x['payload']['after'])
            # kafka_stream.pprint()

            kafka_stream.foreachRDD(lambda rdd: cls.handle_rdds(rdd) \
                if not rdd.isEmpty() else None)

            # Start the context
            cls.ssc.start()
            cls.ssc.awaitTermination()

        except Exception as ex:
            print('KAFKA/SPARK RUN :{}'.format(ex))




