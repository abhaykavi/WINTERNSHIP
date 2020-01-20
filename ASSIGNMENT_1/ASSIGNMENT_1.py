# Databricks notebook source
rdd=sc.textFile("/FileStore/tables/ELEC_PERF.txt")
rdd.collect()

# COMMAND ----------

rdd2=rdd.map(lambda a:a.split(','))
rdd2_elec1=rdd2.map(lambda x: (x[2],1))
rdd2_e1=rdd2_elec1.reduceByKey(lambda x,y: x+y).map(lambda x:(x[1],x[0])).sortByKey(False)
print("The most chosen elective 1 along with the number of students who took it is:")
print(rdd2_e1.take(1))
rdd2_elec2=rdd2.map(lambda x: (x[3],1))
rdd2_e2=rdd2_elec2.reduceByKey(lambda x,y: x+y).map(lambda x:(x[1],x[0])).sortByKey(False)
print("The most chosen elective 2 along with the number of students who took it is:")
print(rdd2_e2.take(1))

# COMMAND ----------

rdd2_comb=rdd2.map(lambda x: ((x[2],x[3]),1))
rdd3=rdd2_comb.reduceByKey(lambda x,y: x+y).map(lambda x:(x[1],x[0])).sortByKey(False)
print("The most chosen combination of electives 2 with the number of students who took that combination is:")
print(rdd3.take(1))

# COMMAND ----------

import statistics
rdd4_elec1=rdd2.map(lambda x: (x[2],int(x[4])))
rdd4_elec1_grouped=rdd4_elec1.groupByKey()
list_e1=list((j[0], statistics.mean(list(j[1]))) for j in rdd4_elec1_grouped.collect())
list_e1_sorted=sorted(list_e1, key = lambda x: x[1],reverse=True)
print("The elective 1 which gave best performance along with it's mean value is:")
print(list_e1_sorted[0])
list_e1_stdev=list((j[0], statistics.stdev(list(j[1]))) for j in rdd4_elec1_grouped.collect())
list_e1_stdev=sorted(list_e1_stdev, key = lambda x: x[1])
print("The elective 1 which has least standard deviation and that we can say it's mean is assured is:")
print(list_e1_stdev[0])

rdd4_elec2=rdd2.map(lambda x: (x[3],int(x[5])))
rdd4_elec2_grouped=rdd4_elec2.groupByKey()
list_e2=list((j[0], statistics.mean(list(j[1]))) for j in rdd4_elec2_grouped.collect())
list_e2_sorted=sorted(list_e2, key = lambda x: x[1],reverse=True)
print("The elective 2 which gave best performance along with it's mean value is:")
print(list_e2_sorted[0])
list_e2_stdev=list((j[0], statistics.stdev(list(j[1]))) for j in rdd4_elec2_grouped.collect())
list_e2_stdev=sorted(list_e2_stdev, key = lambda x: x[1])
print("The elective 2 which has least standard deviation and that we can say it's mean is assured is:")
print(list_e2_stdev[0])

# COMMAND ----------

rdd5=rdd2.map(lambda x: ((x[2],x[3]),(int(x[4]),int(x[5]))))
rdd5_grouped=rdd5.groupByKey()
list_comb=list((j[0], statistics.mean(list(map(sum,j[1])))) for j in rdd5_grouped.collect())
list_comb_sorted=sorted(list_comb, key = lambda x: x[1],reverse=True)
print("The combination of electives which gave best performance along with it's mean value is:")
print(list_comb_sorted[0])
list_comb_stdev=list((j[0], statistics.stdev(list(map(sum,j[1])))) for j in rdd5_grouped.collect())
list_comb_stdev=sorted(list_comb_stdev, key = lambda x: x[1])
print("The elective 2 which has least standard deviation and that we can say it's mean is assured is:")
print(list_comb_stdev[0])

# COMMAND ----------

rdd4_elec1_grouped.collect()
rdd4_elec2_grouped.collect()
