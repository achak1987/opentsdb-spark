name := "opentsdb-spark"

version := "0.2"

scalaVersion := "2.11.6"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.2.0-cdh5.3.0"

libraryDependencies += "org.apache.hbase" % "hbase-server" % "0.98.6-cdh5.3.0"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "0.98.6-cdh5.3.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.5.0-cdh5.3.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.5.0-cdh5.3.0"

resolvers += "Cloudera Repo" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
