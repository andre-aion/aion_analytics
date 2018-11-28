import findspark
findspark.init('/usr/local/spark/spark-2.3.2-bin-hadoop2.7')

from pyspark import SparkContext
from pyspark.sql import *
from pyspark.conf import SparkConf

SparkContext.setSystemProperty('spark.executor.memory', '3g')

conf = SparkConf() \
    .set("spark.streaming.kafka.backpressure.initialRate", 1000)\
    .set("spark.streaming.kafka.backpressure.enabled",'true')\
    .set('spark.streaming.kafka.maxRatePerPartition',20000)


from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from streamz import Stream
from queue import Queue
from dask.distributed import Client


sparkContext = SparkContext().getOrCreate()
spark = SparkSession \
    .builder \
    .appName('poolminer') \
    .master('local[2]') \
    .config(conf=conf) \
    .getOrCreate()

'''
ssc = StreamingContext(sparkContext,0.5) #
ssc.checkpoint('data/sparkcheckpoint')
sqlContext = SQLContext(sparkContext)

lines = ssc.textFileStream()


ssc.start()
ssc.awaitTermination()

'''