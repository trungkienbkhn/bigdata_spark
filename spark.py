from pyspark import SparkContext
from operator import add
from pyspark.sql import Row, SQLContext
sc = SparkContext("local","test app")
sc.setLogLevel('WARN')
#words = sc.textFile("file://///usr/local/spark/3.0.1/bin/wordcount/test.txt")
words = sc.parallelize(["spark","spark context","spark","rabiler in rabiloo"])
word = words.take(2)
# words = sc.parallelize([1,2,3,4,5])
# word = words.reduce(add)
#word = words.filter(lambda x: 'spark' in x)
# def f(x):
#     x = x.lstrip('s')
#     print(x)
# words.foreach(f)
#word = words.flatMap(lambda x: x.split(' ')).map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
# word = words.map(lambda x: (x,1))
#word = words.flatMap(lambda x: x.split(' '))
#for (a,b) in word.collect():
#    print(a,":",b)
#print(word.glom().collect())  
print(word)
