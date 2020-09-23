import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import json
import re

#reload(sys)
#sys.setdefaultencoding('utf-8')

if __name__ == "__main__":
    sc = SparkContext(appName="SparkStreaming")
    sqlContext = SQLContext(sc)
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 5)
    
    broker, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list":broker})
    
    lines = kvs.map(lambda x: x[1])
    
    def readRdd4rmKafkaStream(readRDD):
        if not readRDD.isEmpty():
            df = sqlContext.read.json(readRDD)
            df.show()
            
    lines.foreachRDD(lambda rdd: readRdd4rmKafkaStream(rdd))
    print("\n\n\nWELCOME HOME\n\n\n")
    ssc.start()
    ssc.awaitTermination()
    
    