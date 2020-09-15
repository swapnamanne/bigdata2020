import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    sc = SparkContext(appName="SparkStreaming")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 7)
    
    broker, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list":broker})
    lines = kvs.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a,b: a+b)
    counts.pprint()
    print("\n\n\n\nHELLOOO WORLD\n\n\n\n")
    ssc.start()
    ssc.awaitTermination()
