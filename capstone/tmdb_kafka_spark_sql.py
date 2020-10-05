import json
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.types import *

if __name__ == "__main__":
    sc = SparkContext(appName="tmdbSqlStream")
    sqlContext = SQLContext(sc)
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 5)
    
    spark = SparkSession.builder.appName("tmdbSqlStream").getOrCreate()

    broker, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic],
                                  {"metadata.broker.list":broker})

    lines = kvs.map(lambda x: x[1])
    
    cnt_all = 0
    cnt = 0
    
    def readRdd4rmKafkaStream(readRDD):
        global cnt, cnt_all
        df = sqlContext.read.json(readRDD)
        df.show()
        if not readRDD.isEmpty():
            if cnt < 1:
                mode = "overwrite"
                cnt+=1
            else:
                mode = "append"
            df.write.format('jdbc').options(url='jdbc:mysql://localhost/movies', driver='com.mysql.jdbc.Driver',               dbtable='tmdb_table', user='swapsql', password='swapnika').mode(mode).save()
                                                        
    lines.foreachRDD(lambda rdd: readRdd4rmKafkaStream(rdd))
    print("\n\n\nTMDB MOVIES\n\n\n")
    ssc.start()
    ssc.awaitTermination()
    