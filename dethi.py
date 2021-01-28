from pyspark import SparkContext
sc = SparkContext("local", "first app")
rdd = sc.parallelize([("a",1),("b",1),("a",2),("a",8),("c",4),("a",12),("a",18),("c",14)],3)
def mystr(d):
    return d
def myconcat(a,b):
    return a+b 
def mypartConcat(a,b):
    return a+b
sc.setLogLevel('WARN')
print(rdd.combineByKey(mystr,myconcat,mypartConcat).collect())
