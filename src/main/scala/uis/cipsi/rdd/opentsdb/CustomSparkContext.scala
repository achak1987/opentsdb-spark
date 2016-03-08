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
  def create(sparkMaster:String = "local"): SparkContext = {
    //creating spark context
    val sparkConf = new SparkConf()
    sparkConf.setAppName("opentsdb-spark")    
//    sparkConf.set("spark.cores.max", cores)
//    sparkConf.set("spark.executor.memory", memory)
//    sparkConf.set("spark.driver.host", driverHost)
//    sparkConf.set("spark.driver.port", driverPort)    
        
   if (!SparkContext.jarOfClass(this.getClass).isEmpty) {
      //If we run from eclipse, this statement doesnt work!! Therefore the else part
      sparkConf.setJars(SparkContext.jarOfClass(this.getClass).toSeq)
    } else {
      //if we run from eclipse
      sparkConf.setMaster(sparkMaster)    
      val jar = Jar
      val classPath = this.getClass.getResource("/" + this.getClass.getName.replace('.', '/') + ".class").toString()
      println(classPath)
      val sourceDir = classPath.substring("file:".length, classPath.indexOf("/bin") + "/bin".length).toString()

      jar.create(File("/tmp/opentsdb-spark-1.0.jar"), Directory(sourceDir), "opentsdb-spark")
      sparkConf.setJars("/tmp/opentsdb-spark-1.0.jar" :: Nil)
    }
    val sc = new SparkContext(sparkConf)
    sc
  }

}