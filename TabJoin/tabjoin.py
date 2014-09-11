from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("TableJoin").setMaster('local')
sc = SparkContext(conf=conf)

def Mapper(s):
	return (s.split(',')[0],s.split(',')[1])

dist1=sc.textFile('./file01')
dist2=sc.textFile('./file02')

d1=dist1.map(Mapper)
d2=dist2.map(Mapper)

lst=d1.join(d2).collect()

for row in lst:
	print row
