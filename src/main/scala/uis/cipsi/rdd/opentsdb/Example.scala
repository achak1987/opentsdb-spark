/**
 * Test Access to OpenTSDB
 */
package uis.cipsi.rdd.opentsdb

import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

/**
 * @author antorweep chakravorty
 *
 */
object Example {
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  def main(args: Array[String]) {

    if (args.length != 6) {
      println("Params. found(" + args.length + "): " + args.toSeq)
      println("Required params.: zkqourum zkport metric tagkeyval startdate enddate")
      System.exit(1)
    }

    val zookeeperQuorum = args(0) //"ip.or.hostname"
    val zookeeperClientPort = args(1) //"zookeeper port"
    val metric = args(2) //"Metric.Name"  
    val tagVal = args(3) //"tag.key->tag.value" (can also be * or tag.key->*)
    val startD = args(4) //"ddmmyyyyhh:mm" (or can be *)
    val endD = args(5) //"ddmmyyyyhh:mm" (or can be *)

    println("metric=" + metric + ", tagvalues=" + tagVal + ", startdate= " + startD + ", enddate=" + endD)

    val sc = new SparkContext

    //Connection to OpenTSDB
    val sparkTSDB = new SparkTSDBQuery(zookeeperQuorum, zookeeperClientPort)
    //Create RDD from OpenTSDB
    val data = sparkTSDB.generateRDD(metricName = metric, tagKeyValueMap = tagVal, startdate = startD, enddate = endD, sc)

    //Collect & Print the data
    println("RDD")
    data.collect.foreach(println)

    //Total number of points
    println("Total datapoint= " + data.count)

    sc.stop
  }

}