# Databricks notebook source
rdd=sc.textFile("/FileStore/tables/ELEC_PERF.txt")
rdd.collect()

# COMMAND ----------

rdd2=rdd.map(lambda a:a.split(','))
#rdd2.take(5)
rdd2_elec1=rdd2.map(lambda x: (x[2],1))
print(rdd2_elec1.reduceByKey(lambda x,y: x+y).map(lambda x:(x[1],x[0])).sortByKey(False).collect())
rdd2_elec2=rdd2.map(lambda x: (x[3],1))
print(rdd2_elec2.reduceByKey(lambda x,y: x+y).map(lambda x:(x[1],x[0])).sortByKey(False).collect())

# COMMAND ----------

rdd2_comb=rdd2.map(lambda x: ((x[2],x[3]),1))
#print(rdd2_comb.collect())
rdd3=rdd2_comb.reduceByKey(lambda x,y: x+y).map(lambda x:(x[1],x[0])).sortByKey(False)
print(rdd3.collect())

# COMMAND ----------

import statistics
rdd4_elec1=rdd2.map(lambda x: (x[2],int(x[4])))
rdd4_elec1_grouped=rdd4_elec1.groupByKey()
print(list((j[0], statistics.mean(list(j[1]))) for j in rdd4_elec1_grouped.collect()))
print(list((j[0], statistics.stdev(list(j[1]))) for j in rdd4_elec1_grouped.collect()))
rdd4_elec2=rdd2.map(lambda x: (x[3],int(x[5])))
rdd4_elec2_grouped=rdd4_elec2.groupByKey()
print(list((j[0], statistics.mean(list(j[1]))) for j in rdd4_elec2_grouped.collect()))
print(list((j[0], statistics.stdev(list(j[1]))) for j in rdd4_elec2_grouped.collect()))

# COMMAND ----------

rdd5=rdd2.map(lambda x: ((x[2],x[3]),(int(x[4]),int(x[5]))))
rdd5_grouped=rdd5.groupByKey()
print(list((j[0], statistics.mean(list(map(sum,j[1])))) for j in rdd5_grouped.collect()))
print(list((j[0], statistics.stdev(list(map(sum,j[1])))) for j in rdd5_grouped.collect()))

# COMMAND ----------

rdd4_elec1_grouped.collect()
rdd4_elec2_grouped.collect()
