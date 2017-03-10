package com.mapr.sparkdb.tools.importjson

import com.mapr.db.{Admin, MapRDB, TableDescriptor}
import org.apache.spark.{SparkConf, SparkContext}
import com.mapr.db.spark._
import com.mapr.sparkdb.tools.common.SparkToolsConstants._
import com.typesafe.config._
import com.mapr.sparkdb.tools.common.{RunnerInfo, Utils}
import org.ojai.Document
import org.apache.hadoop.conf.Configuration
import org.ojai.json.mapreduce.JSONFileInputFormat
import org.apache.hadoop.io.LongWritable
/**
  * Created by aravi on 3/8/17.
  */
object ImportJSON {

  val appName = "ImportJSON"
  def main(args: Array[String]): Unit = {
    try {
      //Parse arguments
      implicit val runInfo = parseArgs(args)
      println("Source File: " + runInfo.source)
      println("Sink Table: " + runInfo.sink)

      //Setup spark context
      val config: Config = ConfigFactory.load("application")
      val conf: SparkConf = new SparkConf()
        .setAppName(appName)
        .setSparkHome(config.getString(SPARK_HOME_CONFIG))
        .set(SERIALIZER_CONFIG, config.getString(SERIALIZER_CONFIG))
        .set(KRYO_REGISTRATOR_CONFIG, config.getString(KRYO_REGISTRATOR_CONFIG))
      implicit val sc: SparkContext = SparkContext.getOrCreate(conf)

      println("SparkContext config: " + sc.getConf.toDebugString)

      implicit val hadoopConf = Utils.createHadoopConfiguration()

      //Setup sink table
      val isNew = setupSinkTable(runInfo.sink)

      try {
        runImport
      } catch {
        case e: Throwable => {
          e.printStackTrace()
          if(isNew)
            Utils.unsetBulkLoad(runInfo.sink)
        }
      }

      //Ensure bulkload is set to false for a new table
      if(isNew)
        Utils.unsetBulkLoad(runInfo.sink)

    } catch {
      case e: Throwable => e.printStackTrace()
    }
  }

  private[importjson] def runImport(implicit sc: SparkContext, conf: Configuration, runInfo: RunnerInfo): Unit = {
    sc.newAPIHadoopFile(runInfo.source,
      classOf[JSONFileInputFormat],
      classOf[LongWritable],
      classOf[Document],
      conf)
      .map(x => x._2)
      .map(ojaiDoc => MapRDBSpark.newDocument(ojaiDoc))
      .saveToMapRDB(runInfo.sink,
        createTable = false,
        bulkInsert = true,
        idFieldPath = "_id")
  }

  private[importjson] def parseArgs(args: Array[String]): RunnerInfo = {
    var src: String = ""
    var sink: String = ""
    args foreach {
      case(value) =>
        value match {
          case "-src" => src = args(args.indexOf(value)+1)
          case "-sink" => sink = args(args.indexOf(value)+1)
          case _ => if(value.startsWith("-"))
            println(s"[WARN] - Unrecognized argument $value")
        }
    }
    if(src.isEmpty || sink.isEmpty) {
      usage()
    }
    RunnerInfo(src, sink)
  }

  private[importjson] def usage(): Unit = {
    println(s"Usage: $appName -src <Input text file/directory path> -sink <MapRDB-JSON sink table path>")
    System.exit(1)
  }

  /**
    * Sets up the destination / sink table.
    * If table exists, it is not touched. Else, create sink table
    * @param sinkPath - Path to the sink / destination table.
    * @throws java.io.IOException - Throws DBExceptions.
    * @return - Returns true if sink table was created, else false.
    */
  @throws(classOf[java.io.IOException])
  private [importjson] def setupSinkTable(sinkPath: String): Boolean = {
    val admin: Admin = MapRDB.newAdmin()
    try {
      if (!admin.tableExists(sinkPath)) {
        val tableDesc: TableDescriptor = MapRDB.newTableDescriptor(sinkPath)
          .setPath(sinkPath)
          .setBulkLoad(true)

        admin.createTable(tableDesc)
        return true
      }
      false
    } catch {
      case e: Exception => {
        throw new java.io.IOException(e.getMessage, e.getCause)
      }
    } finally {
      admin.close()
    }
  }
}
