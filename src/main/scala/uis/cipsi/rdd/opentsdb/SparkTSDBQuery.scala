package uis.cipsi.rdd.opentsdb

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import scala.collection.mutable.ArrayBuffer
import java.util.Arrays
import java.nio.ByteBuffer
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import scala.Array.canBuildFrom

class SparkTSDBQuery(sMaster: String, zkQuorum: String, zkClientPort: String) {
  private val sparkMaster = sMaster
  private val zookeeperQuorum = zkQuorum
  private val zookeeperClientPort = zkClientPort
  private val format_data = new java.text.SimpleDateFormat("ddMMyyyyHH:mm")

  def generalConfig(zookeeperQuorum: String, zookeeperClientPort: String): Configuration = {
    //Create configuration
    val config = HBaseConfiguration.create()
    //config.addResource(System.getenv("HBASE_HOME") + "/conf/hbase-site.xml")      
    config.set("hbase.zookeeper.quorum", zookeeperQuorum)
    config.set("hbase.zookeeper.property.clientPort", zookeeperClientPort)
    config.set("hbase.mapreduce.scan.column.family", "t")
    config
  }

  //Prepares the configuration for querying the TSDB-UID table and extracting UIDs for metrics and tags
  def tsdbuidConfig(zookeeperQuorum: String, zookeeperClientPort: String, columnQ: Array[String]) = {
    val config = generalConfig(zookeeperQuorum, zookeeperClientPort)
    config.set("hbase.mapreduce.inputtable", "tsdb-uid")
    config.set("net.opentsdb.tsdb.uid", columnQ.mkString("|"))
    config
  }

  //Prepares the configuration for querying the TSDB table
  def tsdbConfig(zookeeperQuorum: String, zookeeperClientPort: String,
    metric: Array[Byte], tagkv: Option[Array[Byte]] = None,
    startdate: Option[String] = None, enddate: Option[String] = None) = {

    val config = generalConfig(zookeeperQuorum, zookeeperClientPort)
    config.set("hbase.mapreduce.inputtable", "tsdb")
    config.set("net.opentsdb.rowkey", bytes2hex(metric, "\\x"))
    if (tagkv != None) {
      config.set("net.opentsdb.tagkv", bytes2hex(tagkv.get, "\\x"))
    }
    
    val stDateBuffer = ByteBuffer.allocate(4)
    stDateBuffer.putInt((format_data.parse(if (startdate != None) startdate.get else "0101197001:00").getTime() / 1000).toInt)

    val endDateBuffer = ByteBuffer.allocate(4)
    endDateBuffer.putInt((format_data.parse(if (enddate != None) enddate.get else "3112209923:59").getTime() / 1000).toInt)

    config.set("hbase.mapreduce.scan.timerange.start", bytes2hex(stDateBuffer.array(), "\\x"))
    config.set("hbase.mapreduce.scan.timerange.end", bytes2hex(endDateBuffer.array(), "\\x"))
    config
  }

  //Converts Bytes to Hex (see: https://gist.github.com/tmyymmt/3727124)
  def bytes2hex(bytes: Array[Byte], sep: String): String = {
    val regex = sep + bytes.map("%02x".format(_).toUpperCase()).mkString(sep)
    regex
  }

  //Prepare for any or all tag values
  def prepareTagKeyValue(tagkv: String): String = {
    var rtn = ""
    val size = tagkv.length()
    val keyValueGroupSize = 24 //(6key+6value+(2*6)\x)        	

    for (i <- 0 until size by keyValueGroupSize) {
      rtn += tagkv.slice(i, i + keyValueGroupSize) + "|"
    }
    rtn = rtn.slice(0, rtn.length() - 1)

    rtn
  }

  def generateRDD(metricName: String, tagKeyValueMap: String, startdate: String, enddate: String, sc: SparkContext) = {
    val metric = metricName
    var tags = if (tagKeyValueMap.trim != "*")
      tagKeyValueMap.split(",").map(_.split("->")).map(l => (l(0).trim, l(1).trim)).toMap
    else Map("dummyKey" -> "dummyValue")
    
    val columnQualifiers = Array("metrics", "tagk", "tagv")

    val tsdbUID = sc.newAPIHadoopRDD(tsdbuidConfig(zookeeperQuorum, zookeeperClientPort,
      Array(metric, tags.map(_._1).mkString("|"), tags.map(_._2).mkString("|"))),
      classOf[uis.cipsi.rdd.opentsdb.TSDBInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val metricsUID = tsdbUID.map(l =>
      l._2.getValue("id".getBytes(), columnQualifiers(0).getBytes())).filter(_ != null).collect //Since we will have only one metric uid
    val tagKUIDs = tsdbUID.map(l => (new String(l._1.copyBytes()), l._2.getValue("id".getBytes(), columnQualifiers(1).getBytes()))).filter(_._2 != null).collect.toMap
    val tagVUIDs = tsdbUID.map(l => (new String(l._1.copyBytes()), l._2.getValue("id".getBytes(), columnQualifiers(2).getBytes()))).filter(_._2 != null).collect.toMap
    val tagKKeys = tagKUIDs.keys.toArray
    val tagVKeys = tagVUIDs.keys.toArray
    //If certain user supplied tags were not present 
    tags = tags.filter(kv => tagKKeys.contains(kv._1) && tagVKeys.contains(kv._2))

    val tagKV = tagKUIDs.filter(kv => tags.contains(kv._1)).map(k => (k._2, tagVUIDs(tags(k._1)))).map(l => (l._1 ++ l._2)).toList.sorted(Ordering.by((_: Array[Byte]).toIterable))
    
    if (metricsUID.length == 0) {
      println("Not Found: " + (if (metricsUID.length == 0) "Metric:" + metricName))
      System.exit(1)
    }
    
    val tsdb = sc.newAPIHadoopRDD(tsdbConfig(zookeeperQuorum, zookeeperClientPort, metricsUID.last,
      if (tagKV.size != 0) Option(tagKV.flatten.toArray) else None,
      if (startdate != "*") Option(startdate) else None, if (enddate != "*") Option(enddate) else None),
      classOf[uis.cipsi.rdd.opentsdb.TSDBInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
      
    //Decoding retrieved data into RDD
    tsdb.map(kv => (Arrays.copyOfRange(kv._1.copyBytes(), 3, 7),
      kv._2.getFamilyMap("t".getBytes())))
      .map({
        kv =>
          val basetime: Long = ByteBuffer.wrap(kv._1).getInt
          val iterator = kv._2.entrySet().iterator()
          val row = new ArrayBuffer[(Long, Float)]
          var ctr = 0;
          while (iterator.hasNext()) {
            ctr += 1
            val next = iterator.next()
            val a = next.getKey();
            val b = next.getValue();

            def getDelta(a: Array[Byte]): Int = {
              var key = 0;
              for (i <- 0 until a.length) {
                val bo = (a(i) & 0XFF) << ((a.length * 8 - 8) - (i * 8));
                key = key | bo
              }
              key
            }

            //Column Quantifiers(delta) are stored as follows:
            //if number of bytes=2: 12bit value + 4 bit flag
            //if number of bytes=4: first 4bytes flag + next 22 bytes value + last 6 bytes flag
            //if number of bytes>4: columns with compacted for the hour, with each delta having 2 bytes
            //Here we perform bitwise operation to achieve the same
            val delta: Array[Long] = if (a.length == 2) Array(getDelta(a) >> 4)
            else if (a.length == 4) Array((getDelta(a) << 4) >> 6)
            else {
              val deltaB = new ArrayBuffer[Long]()
              for (i <- 0 until a.length by 2) {
                deltaB += getDelta(Array(a(i), a(i + 1))) >> 4
              }
              deltaB.toArray
            }

            val bySize = if (a.length / 2 == (b.length - 1) / 4) 4 else 8
            val value: Array[Float] = if (b.length == 1) {
              val bo = (b(0) & 0XFF) << ((b.length * 8 - 8) - (0 * 8));
              Array(0 | bo)
            } else if (b.length == 4) Array(ByteBuffer.wrap(b).getFloat())
            else {
              val valueB = new ArrayBuffer[Float]()
              for (i <- 0 until b.length by bySize) {
                valueB += ByteBuffer.wrap(Arrays.copyOfRange(b, i, i + 4)).getFloat()
              }
              valueB.toArray
            }

            for (i <- 0 until delta.length)
              row += ((basetime + delta(i), value(i)))
          }
          row
      }).flatMap(_.map(kv => (kv._1, kv._2)))

  }
}
