import pyspark
from collections import Counter
conf = pyspark.SparkConf().setAppName('Hashtag_count')
sc = pyspark.SparkContext(conf=conf)
sc.setLogLevel('WARN')
es_read_config = {'es.nodes': '192.168.1.20', 'es.resource': 'hanoi/doc'}
es_rdd = sc.newAPIHadoopRDD(inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
                            keyClass="org.apache.hadoop.io.NullWritable",
                            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
                            conf=es_read_config)
rdd_list = es_rdd.collect()
hashtag = []
for i in range(0,len(rdd_list)):
    t = rdd_list[i][1]['hashtags']
    if t != '[]':
        e = t.split("', '")
        e[0] = e[0].lstrip("['")
        e[len(e)-1] = e[len(e)-1].rstrip("']")
        for j in range(0,len(e)):
            hashtag.append(e[j])

a = Counter(hashtag).most_common(5)
print(a)
