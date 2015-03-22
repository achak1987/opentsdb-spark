package uis.cipsi.rdd.opentsdb

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.tools.nsc.io.Jar
import scala.tools.nsc.io.File
import scala.tools.nsc.io.Directory
import scala.Option.option2Iterable
import scala.reflect.io.Path.string2path

/**
 * @author antorweepchakravorty
 *
 */
object CustomSparkContext {
  def create(sparkMaster:String = "local",
      zookeeperQuorum:String = "localhost",
      zookeeperClientPort:String = "2181", cores:String ="2", memory:String ="512m", driverHost: String, driverPort:String): SparkContext = {
    //creating spark context
    val sparkConf = new SparkConf()
    sparkConf.setAppName("SaferAnalytics")
    sparkConf.setMaster(sparkMaster)
    sparkConf.set("spark.cores.max", cores)
    sparkConf.set("spark.executor.memory", memory)
    sparkConf.set("spark.driver.host", driverHost)
    sparkConf.set("spark.driver.port", driverPort)    
        
    if (!SparkContext.jarOfClass(this.getClass).isEmpty) {
      //If we run from eclipse, this statement doesnt work!! Therefore the else part
      sparkConf.setJars(SparkContext.jarOfClass(this.getClass).toSeq)
    } else {
      val jar = Jar
      val classPath = this.getClass.getResource("/" + this.getClass.getName.replace('.', '/') + ".class").toString()      
      val sourceDir = classPath.substring("file:".length, classPath.indexOf("uis/cipsi/rdd/opentsdb")).toString()
      jar.create(File("/tmp/opentsdb-spark.jar"), Directory(sourceDir), "opentsdb-spark")      
      sparkConf.setJars(Seq("/tmp/opentsdb-spark.jar"))
    }
   println(sparkMaster)
    val sc = new SparkContext(sparkConf)
     println("heres")
     System.exit(1)
    sc
  }

}