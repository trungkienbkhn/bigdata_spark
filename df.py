from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark import sql

sc = SparkContext("local","df")
sc.setLogLevel('WARN')
sqlContext = sql.SQLContext(sc)

a = sc.parallelize([Row(name='Alice', age=5, height=80), 
                     Row(name='Kien', age=10, height=80)])
df = a.toDF()
df.show()
print(df)