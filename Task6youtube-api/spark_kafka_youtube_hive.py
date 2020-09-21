import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from importlib import reload
import re
import json

#reload(sys)
#sys.setdefaultencoding('utf-8')

if __name__ == "__main__":
    sc = SparkContext(appName="youtubeHiveStream")
    sqlContext = SQLContext(sc)
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 5)
    
    spark = SparkSession.builder\
       .appName("youtubeHiveStream")\
       .config("spark.sql.warehouse.dir","/usr/hive/warehouse")\
       .config("spark.sql.catalogImplementation","hive")\
       .config("hive.metastore.uris","thrift://localhost:9083")\
       .enableHiveSupport()\
       .getOrCreate()\
    
    spark.sql("DROP TABLE IF EXISTS youtube_data")
    
    broker, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list":broker})
    
    lines = kvs.map(lambda x: x[1])
    
    cnt_all = 0
    cnt = 0
    
    def readRdd4rmKafkaStream(readRDD):
        global cnt, cnt_all
        if not readRDD.isEmpty():
            df = sqlContext.read.json(readRDD)
            #df.show()
            snippet_df = df.select("snippet.*","*")
            snippet_df.show()
            
            if cnt < 1:
                snippet_df.write.mode("overwrite")\
                      .saveAsTable("bigdata.youtube_data")
                cnt+=1
            else:
                snippet_df.write.mode("append")\
                      .saveAsTable("bigdata.youtube_data")
    
    lines.foreachRDD(lambda rdd: readRdd4rmKafkaStream(rdd))
    print("\n\n\nBIGDATA WORLD\n\n\n")
    ssc.start()
    ssc.awaitTermination()
   