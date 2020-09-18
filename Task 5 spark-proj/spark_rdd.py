from pyspark import SparkContext
from pyspark.sql.functions import col, split

sc = SparkContext("local","textfile")
lines = sc.textFile("file:///home/swap/Documents/Shakespeare.txt")

type(lines)

lines.take(15)

local_file_rdd = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word,1)).reduceByKey(lamda a,b: a+b)
locai_file_rdd.take(40)
