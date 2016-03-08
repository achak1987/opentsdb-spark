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
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import scala.collection.mutable.Map
import org.apache.spark.rdd.RDD

class SparkTSDBQuery(zkQuorum: String, zkClientPort: String) {
  private val zookeeperQuorum = zkQuorum
  private val zookeeperClientPort = zkClientPort
  private val format_data = new java.text.SimpleDateFormat("ddMMyyyyHH:mm")

  def generalConfig(zookeeperQuorum: String, zookeeperClientPort: String): Configuration = {
    //Create configuration
    val config = HBaseConfiguration.create()
    //config.addResource(System.getenv("HBASE_HOME") + "/conf/hbase-site.xml")      
    config.set("hbase.zookeeper.quorum", zookeeperQuorum)
    config.set("hbase.zookeeper.property.clientPort", zookeeperClientPort)
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

    config.set("net.opentsdb.tsdb.scan.timerange.start", bytes2hex(stDateBuffer.array(), "\\x"))
    config.set("net.opentsdb.tsdb.scan.timerange.end", bytes2hex(endDateBuffer.array(), "\\x"))
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

  def generateRDD(metricName: String, tagKeyValueMap: String, startdate: String, enddate: String, sc: SparkContext): RDD[(Long, Double)] = {
    val metric = metricName
    var tags = if (tagKeyValueMap.trim != "*")
      tagKeyValueMap.split(",").map(_.split("->")).map(l => (l(0).trim, l(1).trim)).toMap
    else Map("dummyKey" -> "dummyValue")

    val columnQualifiers = Array("metrics", "tagk", "tagv")

    val conf = HBaseConfiguration.create()

    conf.set(TableInputFormat.INPUT_TABLE, "tsdb-uid")

    val tsdbUIDConf = tsdbuidConfig(zookeeperQuorum, zookeeperClientPort,
      Array(metric, tags.map(_._1).mkString("|"), tags.map(_._2).mkString("|")))

    val tsdbUID = sc.newAPIHadoopRDD(tsdbUIDConf, classOf[uis.cipsi.rdd.opentsdb.TSDBInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val metricsUID = tsdbUID.map(l =>
      l._2.getValue("id".getBytes(), columnQualifiers(0).getBytes())).filter(_ != null).collect //Since we will have only one metric uid

    val tagKUIDs = tsdbUID.map(l => (new String(l._1.copyBytes()), l._2.getValue("id".getBytes(), columnQualifiers(1).getBytes()))).filter(_._2 != null).collect.toMap
    val tagVUIDs = tsdbUID.map(l => (new String(l._1.copyBytes()), l._2.getValue("id".getBytes(), columnQualifiers(2).getBytes()))).filter(_._2 != null).collect.toMap
    val tagKKeys = tagKUIDs.keys.toArray
    val tagVKeys = tagVUIDs.keys.toArray

    //If certain user supplied tags were not present 
    tags = tags.filter(kv => tagKKeys.contains(kv._1) && tagVKeys.contains(kv._2))

    val tagKV = tagKUIDs.filter(kv => tags.contains(kv._1))
      .map(k => (k._2, tagVUIDs(tags(k._1))))
      .map(l => (l._1 ++ l._2))
      .toList.sorted(Ordering.by((_: Array[Byte]).toIterable))

    if (metricsUID.length == 0) {
      println("Not Found: " + (if (metricsUID.length == 0) "Metric:" + metricName))
      System.exit(1)
    }

    conf.set(TableInputFormat.INPUT_TABLE, "tsdb")

    val tsdbConf = tsdbConfig(zookeeperQuorum, zookeeperClientPort, metricsUID.last,
      if (tagKV.size != 0) Option(tagKV.flatten.toArray) else None,
      if (startdate != "*") Option(startdate) else None, if (enddate != "*") Option(enddate) else None)

    val tsdb = sc.newAPIHadoopRDD(tsdbConf, classOf[uis.cipsi.rdd.opentsdb.TSDBInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    //Decoding retrieved data into RDD
    val out = tsdb.map(kv => (Arrays.copyOfRange(kv._1.copyBytes(), 3, 7),
      kv._2.getFamilyMap("t".getBytes())))
      .map({
        kv =>
          val row = new ArrayBuffer[(Long, Double)]
          val basetime: Long = ByteBuffer.wrap(kv._1).getInt
          val iterator = kv._2.entrySet().iterator()
          var ctr = 0;
          while (iterator.hasNext()) {
            ctr += 1
            val next = iterator.next()
            val a = next.getKey();
            val b = next.getValue();

            def processQuantifier(quantifier: Array[Byte]): Array[(Int, Boolean, Int)] = {
              //converting Byte Arrays to a Array of binary string
              val q = quantifier.map({ v => Integer.toBinaryString(v & 255 | 256).substring(1) })
              var i = 0
              val out = new ArrayBuffer[(Int, Boolean, Int)]
              while (i != q.length) {
                var value = -1
                var isInteger = true
                var valueLength = -1
                var isQuantifierSizeTypeSmall = true
                //If the 1st 4 bytes are in format "1111", the size of the column quantifier is 4 bytes. Else 2 bytes
                if (q(i).substring(0, 3).compareTo("1111") == 0) {
                  isQuantifierSizeTypeSmall = false
                }

                if (isQuantifierSizeTypeSmall) {
                  val v = q(i) + q(i + 1).substring(0, 4) //The 1st 12 bits represent the delta
                  value = Integer.parseInt(v, 2) //convert the delta to Int (seconds)                  
                  isInteger = q(i + 1).substring(4, 5) == "0" //The 13th bit represent the format of the value for the delta. 0=Integer, 1=Float
                  valueLength = Integer.parseInt(q(i + 1).substring(5, 8), 2) //The last 3 bits represents the length of the value
                  i = i + 2
                } else {
                  val v = q(i).substring(4, 8) + q(i + 1) + q(i + 2) + q(i + 3).substring(0, 2) //The first 4 bits represents the size, the next 22 bits hold the delta
                  value = Integer.parseInt(v, 2) / 1000 //convert the delta to Int (milliseconds -> seconds)
                  isInteger = q(i + 3).substring(4, 5) == "0" //The 29th bit represent the format of the value for the delta. 0=Integer, 1=Float
                  valueLength = Integer.parseInt(q(i + 3).substring(5, 8), 2) //The last 3 bits represents the length of the value
                  i = i + 4
                }

                out += ((value, isInteger, valueLength + 1))
              }
              out.toArray
            }

            def processValues(quantifier: Array[(Int, Boolean, Int)], values: Array[Byte]): Array[Double] = {
              //converting Byte Arrays to a Array of binary string
              val v = values.map({ v => Integer.toBinaryString(v & 255 | 256).substring(1) }).mkString

              val out = new ArrayBuffer[Double]
              var i = 0
              var j = 0
              while (j < quantifier.length) {
                //Is the value represented as integer or float
                val isInteger = quantifier(j)._2
                //The number of Byte in which the value has been encoded
                val valueSize = quantifier(j)._3 * 8
                //Get the value for the current delta
                val _value = v.substring(i, i + valueSize).mkString
                val value = (if (!isInteger) {
                  //See https://blog.penjee.com/binary-numbers-floating-point-conversion/
                  val sign = if (_value.substring(0, 1).compare("0") == 0) 1 else -1;
                  val exp = Integer.parseInt(_value.substring(1, 9), 2) - 127
                  val significand = 1 + _value.substring(9).map({ var i = 0; m => i += 1; m.asDigit / math.pow(2, i) }).sum
                  sign * math.pow(2, exp) * significand
                } else Integer.parseInt(_value, 2).toDouble)

                i += valueSize
                j += 1

                out += value
              }
              out.toArray
            }

            val _delta = processQuantifier(a)
            val delta = _delta.map(_._1)
            val value = processValues(_delta, b)

            for (i <- 0 until delta.length)
              row += ((basetime + delta(i), value(i)))
          }
          row
      }).flatMap(_.map(kv => (kv._1, kv._2)))
    out
  }
}
