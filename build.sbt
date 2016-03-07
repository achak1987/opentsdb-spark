name := "opentsdb-spark"

version := "0.2"

scalaVersion := "2.10.6"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.0"

libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.2.0"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.2.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.6.2"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.2"