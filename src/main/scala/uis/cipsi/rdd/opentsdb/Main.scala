/**
 *Test Access to OpenTSDB
 */
package uis.cipsi.rdd.opentsdb

import org.apache.spark.SparkContext

/**
 * @author antorweep chakravorty
 *
 */
object Main {
  def main(args : Array[ String ]) {
            
    if(args.length != 9) {
      println("Required params.: sparkmaster zkqourum zkport metric tagkeyval startdate enddate driverhost driverport")
      System.exit(1)
    }
    
    val sparkMaster = args(0) //"spark://ip.or.hostname:port" //"local (for localhost)"
    val zookeeperQuorum = args(1) //"ip.or.hostname"
    val zookeeperClientPort = args(2) //"zookeeper port"
    val metric = args(3) //"Metric.Name"  
    val tagVal = args(4) //"tag.key->tag.value" (can also be * or tag.key->*)
    val startD = args(5) //"ddmmyyyyhh:mm" (or can be *)
    val endD = args(6) //"ddmmyyyyhh:mm" (or can be)
    val driverHost = args(7) //driver.host.name (or the client which access the cluster; command hostname can be used in terminal to find the host name)
    val driverPort = args(8) //driver port (can be any open port)
    
    val sc = CustomSparkContext.create(sparkMaster = sparkMaster,
      zookeeperQuorum = zookeeperQuorum,
      zookeeperClientPort = zookeeperClientPort,
      driverHost = driverHost,
      driverPort = driverPort)

    //Connection to OpenTSDB
    val sparkTSDB = new SparkTSDBQuery(sparkMaster, zookeeperQuorum, zookeeperClientPort)
    //Create RDD from OpenTSDB
    val data = sparkTSDB.generateRDD(metricName = metric, tagKeyValueMap = tagVal, startdate = startD, enddate = endD, sc)
    .map(kv => (kv._1, Array(kv._2)) ) 
    
    //Collect & Print the data
    data.collect.foreach(println)
    
    //Total number of points
    println(data.count)
    
    sc.stop
  }

}