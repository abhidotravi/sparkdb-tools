package com.mapr.sparkdb.tools.exportjson

import org.apache.spark.{SparkConf, SparkContext}
import com.mapr.db.spark._
import com.mapr.sparkdb.tools.common.SparkToolsConstants._
import com.typesafe.config._
import com.mapr.sparkdb.tools.common.{ExportJsonInfo, Utils}
import org.apache.hadoop.mapred._
import org.ojai.json.JsonOptions
/**
  * Created by aravi on 3/7/17.
  */
object ExportJson {
  val appName = "ExportJson"

  def main(args: Array[String]): Unit = {
    try {
      //Parse arguments
      implicit val runInfo = parseArgs(args)
      println("Source File: " + runInfo.source)
      println("Sink Table: " + runInfo.sink)

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
      runExport

    } catch {
      case e: Throwable => e.printStackTrace()
    }
  }

  private[exportjson] def runExport(implicit sc: SparkContext, runInfo: ExportJsonInfo): Unit = {
    sc.loadFromMapRDB(runInfo.source.get)
      .map(doc => {
        val options = new JsonOptions
        doc.asJsonString(options.withTags())
      }).saveAsTextFile(runInfo.sink.get)
  }

  private[exportjson] def parseArgs(args: Array[String]): ExportJsonInfo = {
    var src: Option[String] = None
    var sink: Option[String] = None
    args foreach {
      case(value) =>
        value match {
          case "-src" => src = Some(args(args.indexOf(value)+1))
          case "-sink" => sink = Some(args(args.indexOf(value)+1))
          case _ => if(value.startsWith("-"))
            println(s"[WARN] - Unrecognized argument $value is ignored")
        }
    }
    if(src.isEmpty || sink.isEmpty) {
      usage()
    }
    ExportJsonInfo(src, sink)
  }

  private[exportjson] def usage(): Unit = {
    println(s"Usage: $appName -src <MapRDB-JSON source table path> -sink <Output text file/directory path>")
    System.exit(1)
  }
}
