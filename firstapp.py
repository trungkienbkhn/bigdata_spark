from pyspark import SparkContext
logFile = "file:////home/kien/Documents/20201/big_data/pyspark_tutorial/test.txt"  
sc = SparkContext("local", "first app")
logData = sc.textFile(logFile).cache()
sc.setLogLevel('WARN')
numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()
print("Lines with a: ",numAs)
print("Lines with b: ",numBs)
