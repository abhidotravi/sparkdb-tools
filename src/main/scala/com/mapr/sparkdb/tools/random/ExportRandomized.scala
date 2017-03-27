package com.mapr.sparkdb.tools.random

import com.mapr.db.spark._
import com.mapr.db.spark.serializers.DBBinaryValueSerializer
import com.mapr.sparkdb.tools.common.SparkToolsConstants._
import com.mapr.sparkdb.tools.common.{ExportJsonInfo, Utils}
import com.typesafe.config._
import org.apache.spark.{SparkConf, SparkContext}
import org.ojai.json.JsonOptions
import org.apache.spark.Partitioner
/**
  * Created by aravi on 3/7/17.
  */
object ExportRandomized {
  case class ExportRandomizedInfo(source: Option[String],
                                  sink: Option[String],
                                  fieldName: Option[String],
                                  splitNo: Option[Int])
  class CustomPartitioner(partitions: Int) extends Partitioner {
    require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")
    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
      key.hashCode % numPartitions
    }
  }
  val appName = "ExportRandomized"

  def main(args: Array[String]): Unit = {
    try {
      //Parse arguments
      implicit val runInfo = parseArgs(args)
      println("Source File: " + runInfo.source.getOrElse(""))
      println("Sink Table: " + runInfo.sink.getOrElse(""))

      //Load config
      val config: Config = ConfigFactory.load("application")
      val conf: SparkConf = new SparkConf()
        .setAppName(appName)
        .setSparkHome(config.getString(SPARK_HOME_CONFIG))
        .set(SERIALIZER_CONFIG, config.getString(SERIALIZER_CONFIG))
        .set(KRYO_REGISTRATOR_CONFIG, config.getString(KRYO_REGISTRATOR_CONFIG))
      implicit val sc: SparkContext = SparkContext.getOrCreate(conf)

      println("SparkContext config: " + sc.getConf.toDebugString)

      //Ensure bulkload is set to false on the source table
      Utils.unsetBulkLoad(runInfo.source.get)
      runExportRandom

    } catch {
      case e: Throwable => e.printStackTrace()
    }
  }
  private[random] def runExportRandom(implicit sc: SparkContext, runInfo: ExportRandomizedInfo): Unit = {
    val options: JsonOptions = new JsonOptions
    sc.loadFromMapRDB(runInfo.source.get)
      .keyBy(x => x.getBinarySerializable(runInfo.fieldName.getOrElse("field1")))
      //.repartitionAndSortWithinPartitions(new CustomPartitioner(runInfo.splitNo.getOrElse(1000)))
      .repartition(runInfo.splitNo.get * 2).repartitionAndSortWithinPartitions()
      .sortByKey()
      //.map(x => (x.getBinarySerializable(runInfo.fieldName.getOrElse("field1")), x))
      //.repartition(runInfo.splitNo.getOrElse(2))
      .map(x => x._2)
      .map(x => {
        val options = new JsonOptions
        x.asJsonString(options.setWithTags(true))
      })
      .saveAsTextFile(runInfo.sink.get)
  }

  private[random] def parseArgs(args: Array[String]): ExportRandomizedInfo = {
    var src: Option[String] = None
    var sink: Option[String] = None
    var field: Option[String] = None
    var splitNo: Option[Int] = None
    args foreach {
      case(value) =>
        value match {
          case "-src" => src = {
            if(args.isDefinedAt(args.indexOf(value)+1))
              Some(args(args.indexOf(value)+1))
            else
              None
          }
          case "-sink" => sink = {
            if(args.isDefinedAt(args.indexOf(value)+1))
              Some(args(args.indexOf(value)+1))
            else
              None
          }
          case "-numsplit" => splitNo = {
            if(args.isDefinedAt(args.indexOf(value)+1))
              Some(args(args.indexOf(value)+1).toInt)
            else
              None
          }
          case "-field" => field = {
            if(args.isDefinedAt(args.indexOf(value)+1))
              Some(args(args.indexOf(value)+1))
            else
              None
          }
          case _ => if(value.startsWith("-"))
            println(s"[WARN] - Unrecognized argument $value is ignored")
        }
    }
    if(src.isEmpty || sink.isEmpty) {
      usage()
    }
    ExportRandomizedInfo(src, sink, field, splitNo)
  }

  private[random] def usage(): Unit = {
    println(s"Usage: $appName -src <MapRDB-JSON source table path> -sink <Output text file/directory path> [Options]")
    println(s"Options:")
    println(s"-numsplit <Number of split files>")
    println(s"-field <Field to sort, for randomization>")
    System.exit(1)
  }
}
