package com.mapr.sparkdb.tools.exporttable

import org.apache.spark.{SparkConf, SparkContext}
import com.mapr.db.spark._
import com.mapr.db._
import com.mapr.sparkdb.tools.common.SparkToolsConstants._
import com.typesafe.config._
import org.ojai.Value
import com.mapr.db.impl.{AdminImpl, IdCodec, TabletInfoImpl}
import com.mapr.sparkdb.tools.common._
import com.mapr.sparkdb.tools.common.Utils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.mapred._

import scala.util.{Failure, Success, Try}
/**
  * Created by aravi on 3/7/17.
  */
object ExportTableRunner {
  @throws(classOf[FileAlreadyExistsException])
  def main(args: Array[String]): Unit = {
    //Get args
    val srcPath: String = Option(args(0))
      .getOrElse("/tables/usertable")
    val sinkDir: String = Option(args(1)).getOrElse("/tables/sinktable/")

    //Load config
    val config: Config = ConfigFactory.load("application")
    val appName: String = "ExportTable"
    val conf: SparkConf = new SparkConf()
      .setAppName(appName)
      .setSparkHome(config.getString(SPARK_HOME_CONFIG))
      .set(SERIALIZER_CONFIG,config.getString(SERIALIZER_CONFIG))
      .set(KRYO_REGISTRATOR_CONFIG, config.getString(KRYO_REGISTRATOR_CONFIG))
    implicit val sc: SparkContext = SparkContext.getOrCreate(conf)

    println("SparkContext config: " + sc.getConf.toDebugString)

    Utils.unsetBulkLoad(srcPath)

    sc
      .loadFromMapRDB(srcPath).map(doc => doc.asJsonString()).saveAsTextFile(sinkDir)
    sc.stop
  }
}
