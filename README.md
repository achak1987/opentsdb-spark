opentsdb-spark
==============

Module for accessing OpenTSDB data through Spark.

#Installation.
##On your client (SparkDriver)
  >Execute the following in your terminal
  1. wget https://github.com/achak1987/opentsdb-spark/archive/master.zip
  2. unzip master.zip
  3. cd opentsdb-spark-master
  4. sbt clean eclipse (if you dont have sbt, it can be installed from www.scala-sbt.org)
  5. The project can be now imported into eclipse

##On your cluster, ensure that you have HBASE on your Apache Spark class path

# Example Execute
```
./spark-submit \
--master spark://host.name:7077 \
--class uis.cipsi.rdd.opentsdb.Main \
/path/to/your/jar/opentsdb-spark-1.0.jar \
spark://ip.or.hostname:port \
zookeeper.ip.or.hostname \
zookeeper.port metric.name \
tag.key->tag.value (can also be * or tag.key->tag.value, or tag.key-> * or tag.key1->tag.value1,tag.key2->tag.value2,...) \
opentsdb.start.date (as ddmmyyyyhh:mm or can be *) \
opentsdn.end.date (as ddmmyyyyhh:mm or can be *) \
```
# Tested with
  apache spark 1.6.0, apache hbase 1.2.0, apache hadoop 2.6.2, opentsdb 2.2.0

# Test Results
## Prepare example data set
1. Make sure that your opentsdb server is up and running
2. Create a metrics with 
  * ./tsdb mkmetric example.cpu.user
3. on your terminal execute
  * nc opentsdb.hostname opentsdb.port
  * copy and paste the following
  ```
  put example.cpu.user 1457367300 42.5 host=webserver00 cpu=0
  put example.cpu.user 1457368200 52 host=webserver00 cpu=0
  
  put example.cpu.user 1457367300 40 host=webserver01 cpu=0
  put example.cpu.user 1457368200 31.5 host=webserver01 cpu=0
  put example.cpu.user 1457369100 46.5 host=webserver01 cpu=0
  
  put example.cpu.user 1457367600 49 host=webserver01 cpu=1
  put example.cpu.user 1457368200 72 host=webserver01 cpu=1
  
  put example.cpu.user 1457367600 52.3 host=webserver02 cpu=0
  put example.cpu.user 1457369100 57.9 host=webserver02 cpu=0
  
  put example.cpu.user 1457367300 41 host=webserver02 cpu=1

  put example.cpu.user 1457367900 -40.5 host=webserver02 cpu=1
  
  put example.cpu.user 1457389243 1 host=webserver02 cpu=1
  put example.cpu.user 1457360443 -1 host=webserver02 cpu=1
  ```
4. Press Ctrl + C to exit

## Running (result RDD as (timestamp, value)
1. with metric=example.cpu.user, tagvalues=\*, startdate=\*, enddate=\* 
  
  ```
  RDD
  (1457360443,255.0)
  (1457367300,42.5)
  (1457368200,52.0)
  (1457367300,40.0)
  (1457368200,31.5)
  (1457369100,46.5)
  (1457367600,49.0)
  (1457368200,72.0)
  (1457367600,52.29999923706055)
  (1457369100,57.900001525878906)
  (1457367300,41.0)
  (1457367900,-40.5)
  (1457389243,1.0)
  Total datapoint= 13
  ```
  
  2. with metric=example.cpu.user, tagvalues=cpu->1, startdate=\*, enddate=\*
 
  ```
  RDD
  (1457360443,255.0)
  (1457367600,49.0)
  (1457368200,72.0)
  (1457367300,41.0)
  (1457367900,-40.5)
  (1457389243,1.0)
  Total datapoint= 6
  ```
  
  3. with metric=example.cpu.user, tagvalues=cpu->1,host->webserver02, startdate=\*, enddate=\*
  
  ```
  RDD
  (1457360443,255.0)
  (1457367300,41.0)
  (1457367900,-40.5)
  (1457389243,1.0)
  Total datapoint= 4
  ```
  
  4. with metric=example.cpu.user, tagvalues=\*, startdate= 0703201617:00, enddate=0703201619:00
  
  ```
  RDD
  (1457367300,42.5)
  (1457368200,52.0)
  (1457367300,40.0)
  (1457368200,31.5)
  (1457369100,46.5)
  (1457367600,49.0)
  (1457368200,72.0)
  (1457367600,52.29999923706055)
  (1457369100,57.900001525878906)
  (1457367300,41.0)
  (1457367900,-40.5)
  Total datapoint= 11
  ```
