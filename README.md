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

#Tested with
  apache spark 1.6.0, apache hbase 1.2.0, apache hadoop 2.6.2, opentsdb 2.2.0

#Test Results
##Prepare example data set
1. Make sure that your opentsdb server is up and running
2. Create a metrics with 
..* ./tsdb mkmetric example.cpu.user
3. on your terminal execute
..* nc opentsdb.hostname opentsdb.port
..* copy and paste the following
....*