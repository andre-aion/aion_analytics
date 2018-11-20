import os
import sys
module_path = os.path.abspath(os.getcwd() + '\\..')
if module_path not in sys.path:
    sys.path.append(module_path)


from pythonCassandra import PythonCassandra
import json
import datetime
import time
from datetime import datetime


import findspark
findspark.init('/usr/local/spark/spark-2.3.2-bin-hadoop2.7')

import pyspark
from pyspark.sql.functions import *
from pyspark import SparkContext, SQLContext
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.conf import SparkConf

SparkContext.setSystemProperty('spark.executor.memory', '3g')

conf = SparkConf() \
    .set("spark.streaming.kafka.backpressure.initialRate", 1000)\
    .set("spark.streaming.kafka.backpressure.enabled",'true')\
    .set('spark.streaming.kafka.maxRatePerPartition',20000)


from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

class KafkaConnectPyspark:
    def __init__(self):
        sparkContext = SparkContext()
        spark = SparkSession \
            .builder \
            .appName('poolminer') \
            .master('local[2]') \
            .config(conf=conf) \
            .getOrCreate()

        self.ssc = StreamingContext(sparkContext,.5) #
        self.ssc.checkpoint('../../data/sparkcheckpoint')
        self.sqlContext = SQLContext(sparkContext)
        self.kafkaStream=None
        self.query=None


    def connect(self):
        def get_month_from_timestamp(ts):
            time = datetime.fromtimestamp(ts)
            return time.month


        def toTuple(taken,pc):
            print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            for mess in taken:
                print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                block_month = get_month_from_timestamp(mess['block_timestamp'])
                message = (mess["block_number"], mess["block_hash"], mess["miner_address"],
                           mess["parent_hash"], mess["receipt_tx_root"],
                           mess["state_root"], mess["tx_trie_root"], mess["extra_data"],
                           mess["nonce"], mess["bloom"], mess["solution"], mess["difficulty"],
                           mess["total_difficulty"], mess["nrg_consumed"], mess["nrg_limit"],
                           mess["size"], mess["block_timestamp"], block_month, mess["num_transactions"],
                           mess["block_time"], mess["nrg_reward"], mess["transaction_id"], mess["transaction_list"])
                print(message)
                pc.insert_data_block([message])

                del message
                del mess
            print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

        def f(rdd):
            if rdd is None:
                print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                print('################  RDD IS NONE')
                print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                return

            # cassandra setup
            pc = PythonCassandra()
            pc.setlogger()
            pc.createsession()
            pc.createkeyspace('aionv4')
            pc.create_table_block()
            taken = rdd.take(20000)
            toTuple(taken,pc)


        topic='mainnetserver.aionv4.block'
        kafkaParams = {"metadata.broker.list": "localhost:9092",
                       "auto.offset.reset": "smallest"}
        kafkaStream = KafkaUtils.createDirectStream(self.ssc,
                                                         [topic], kafkaParams)
        kafkaStream.checkpoint(30)

        kafkaStream = kafkaStream.map(lambda x: json.loads(x[1]))
        kafkaStream = kafkaStream.map(lambda x: x['payload']['after'])
        kafkaStream.pprint()

        kafkaStream.foreachRDD(f)

        self.ssc.start()
        self.ssc.awaitTermination()


if __name__=='__main__':
    kcp = KafkaConnectPyspark()
    kcp.connect()
