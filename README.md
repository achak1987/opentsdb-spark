opentsdb-spark
==============

Module for accessing OpenTSDB data through Spark.

#Installation.
##On your client (SparkDriver)
  >Execute the following in your terminal
  1. wget https://github.com/achak1987/opentsdb-spark/archive/master.zip
  2. unzip master.zip
  3. cd opentsdb-spark-master
  4. sbt eclipse (if you dont have sbt, it can be installed from www.scala-sbt.org)
  5. The project can be now imported into eclipse

##On your cluster.
  >For each node in the cluster add the following into your com file
  1. nano /path/to/your/spark/dir/bin/compute-classpath.sh
  2. copy the following to the end of the file. Before echo "$CLASSPATH"\\
      
      #HBASE home directory
      export HBASE_HOME=/path/to/your/hbase/dir

      #Add HBASE libs to Spark CLASSPATH
      for f in $HBASE_HOME/lib/*.jar
        do
    	CLASSPATH="$CLASSPATH:$f"
      done
      
      #SCALA home directory
      SCALA_HOME=/path/to/your/scala/dir
      
      #Add Scala libs to Spark CLASSPATH
      for f in $SCALA_HOME/lib/*.jar
        do
          CLASSPATH="$CLASSPATH:$f"
        done

#System Info
  Cloudera Hadoop 2.5.0-cdh5.3.0, Cloudera Hbase 0.98.6-cdh5.3.0, Cloudera Spark 1.2.0-cdh5.3.0, Scala 2.11.6, OpenTSDB 2.0.1


