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
object Main {
//  Logger.getLogger("org").setLevel(Level.OFF)
//  Logger.getLogger("akka").setLevel(Level.OFF)
  
  def main(args: Array[String]) {

    if (args.length != 7) {
      println("Required params.: sparkmaster zkqourum zkport metric tagkeyval startdate enddate driverhost driverport")
      System.exit(1)
    }

    val sparkMaster = args(0) //"spark://ip.or.hostname:port" //"local (for localhost)"
    val zookeeperQuorum = args(1) //"ip.or.hostname"
    val zookeeperClientPort = args(2) //"zookeeper port"
    val metric = args(3) //"Metric.Name"  
    val tagVal = args(4) //"tag.key->tag.value" (can also be * or tag.key->*)
    val startD = args(5) //"ddmmyyyyhh:mm" (or can be *)
    val endD = args(6) //"ddmmyyyyhh:mm" (or can be *)

    val sc = CustomSparkContext.create(sparkMaster = sparkMaster)

    //Connection to OpenTSDB
    val sparkTSDB = new SparkTSDBQuery(sparkMaster, zookeeperQuorum, zookeeperClientPort)
    //Create RDD from OpenTSDB
    val data = sparkTSDB.generateRDD(metricName = metric, tagKeyValueMap = tagVal, startdate = startD, enddate = endD, sc)
    println("metric=" + metric + ", tagvalues=" + tagVal + ", startdate= " + startD + ", enddate=" + endD)
    //Collect & Print the data
    println("RDD")
    data.collect.foreach(println)

    //Total number of points
    println("Total datapoint= " + data.count)

    sc.stop
  }

}