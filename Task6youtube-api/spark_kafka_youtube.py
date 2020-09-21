import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json

if __name__ == "__main__":
    sc = SparkContext(appName="youtubestream")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc,5)
    
    broker, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list":broker})
    lines = kvs.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a,b: a+b)
    counts.pprint()
    print("\n\n\nBIGDATA WORLD\n\n\n")
    ssc.start()
    ssc.awaitTermination()
   