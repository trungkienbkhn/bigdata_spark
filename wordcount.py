import sys
from pyspark import SparkContext
from operator import add
sc = SparkContext("local","WordCountExample")
sc.setLogLevel('WARN')
lines = sc.textFile('file:////usr/local/spark/3.0.1/bin/wordcount/test.txt')
counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
output = counts.collect()
for (word, count) in output:
    print ("%s: %i" % (word, count))
sc.stop()
