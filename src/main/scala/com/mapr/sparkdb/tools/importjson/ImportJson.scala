package com.mapr.sparkdb.tools.importjson

import com.mapr.db.{Admin, MapRDB, TableDescriptor}
import org.apache.spark.{SparkConf, SparkContext}
import com.mapr.db.spark._
import com.mapr.sparkdb.tools.common.SparkToolsConstants._
import com.typesafe.config._
import com.mapr.sparkdb.tools.common.{ImportJsonInfo, Utils}
import org.ojai.Document
import org.apache.hadoop.conf.Configuration
import org.ojai.json.mapreduce.JSONFileInputFormat
import org.apache.hadoop.io.LongWritable
/**
  * Created by aravi on 3/8/17.
  */
object ImportJson {

  val appName = "ImportJson"
  def main(args: Array[String]): Unit = {
    try {
      //Parse arguments
      implicit val runInfo = parseArgs(args)
      println("Source File: " + runInfo.source.getOrElse(""))
      println("Sink Table: " + runInfo.sink.getOrElse(""))

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
      implicit val isNew = setupSinkTable(runInfo.sink.get)

      try {
        runImport
      } catch {
        case e: Throwable => {
          e.printStackTrace()
          if(isNew)
            Utils.unsetBulkLoad(runInfo.sink.get)
        }
      }

      //Ensure bulkload is set to false for a new table
      if(isNew)
        Utils.unsetBulkLoad(runInfo.sink.get)

    } catch {
      case e: Throwable => e.printStackTrace()
    }
  }

  private[importjson] def runImport(implicit sc: SparkContext,
                                    conf: Configuration,
                                    runInfo: ImportJsonInfo,
                                    isPresplit: Boolean): Unit = {
    val docRDD = sc.newAPIHadoopFile(runInfo.source.get,
      classOf[JSONFileInputFormat],
      classOf[LongWritable],
      classOf[Document],
      conf)
      .map(x => x._2)
      .map(doc => MapRDBSpark.newDocument(doc))
      .keyBy(doc => doc.getString(runInfo.id.get))

    val sortedRDD = if (isPresplit) {
      docRDD.repartitionAndSortWithinPartitions(
        MapRDBSpark.newPartitioner[String](runInfo.sink.get))
    } else {
      docRDD.sortByKey()
    }

    sortedRDD.saveToMapRDB(runInfo.sink.get,
      createTable = false,
      bulkInsert = true)
  }

  private[importjson] def parseArgs(args: Array[String]): ImportJsonInfo = {
    var src: Option[String] = None
    var sink: Option[String] = None
    var idField: Option[String] = None
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
          case "-id" => {
            idField = {
              if(args.isDefinedAt(args.indexOf(value)+1))
                Some(args(args.indexOf(value)+1))
              else
                None
            }
            if(idField.isEmpty) {
              println("[ERROR]: Value for -id is missing")
              usage()
            }
          }
          case "-h" | "-help" => usage()
          case _ => if(value.startsWith("-")) {
            println(s"[ERROR] - Unrecognized argument $value")
            usage()
          }
        }
    }
    if(src.isEmpty || sink.isEmpty) {
      println("[ERROR] - Mandatory arguments not provided")
      usage()
    }
    ImportJsonInfo(src, sink, Some(idField.getOrElse("_id")))
  }

  private[importjson] def usage(): Unit = {
    println(s"Usage: $appName [Options] -src <Input text file/directory path> -sink <MapRDB-JSON sink table path>")
    println(s"Options:")
    println(s"-h or -help <For usage> ")
    println(s"-id <Field name that will be used as _id the document>")
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
      case e: Exception => throw new java.io.IOException(e.getMessage, e.getCause)
    } finally {
      admin.close()
    }
  }
}
