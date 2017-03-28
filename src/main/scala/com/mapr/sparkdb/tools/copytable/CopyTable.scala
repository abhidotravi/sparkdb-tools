package com.mapr.sparkdb.tools.copytable

import org.apache.spark.{SparkConf, SparkContext}
import com.mapr.db.spark._
import com.mapr.db._
import com.mapr.sparkdb.tools.common.SparkToolsConstants._
import com.typesafe.config._
import com.mapr.db.spark.condition.Predicate
import com.mapr.sparkdb.tools.common.{CopyTableInfo, Utils}

object CopyTable {
  val appName = "CopyTable"

  def main(args: Array[String]): Unit = {
    try {
      //Parse arguments
      implicit val runInfo = parseArgs(args)
      println("Source File: " + runInfo.source.getOrElse(""))
      println("Sink Table: " + runInfo.sink.getOrElse(""))
      println("Start Key: " + runInfo.startKey.getOrElse("-INF"))
      println("End Key: " + runInfo.endKey.getOrElse("INF"))

      //Load config
      val config: Config = ConfigFactory.load("application")
      val conf: SparkConf = new SparkConf()
        .setAppName(appName)
        .setSparkHome(config.getString(SPARK_HOME_CONFIG))
        .set(SERIALIZER_CONFIG, config.getString(SERIALIZER_CONFIG))
        .set(KRYO_REGISTRATOR_CONFIG, config.getString(KRYO_REGISTRATOR_CONFIG))
      implicit val sc: SparkContext = SparkContext.getOrCreate(conf)

      println("SparkContext config: " + sc.getConf.toDebugString)

      Utils.unsetBulkLoad(runInfo.source.get)
      val isNew: Boolean = setupSinkTable(runInfo.source.get, runInfo.sink.get)
      runCopy(isNew)
    } catch {
      case e: Throwable => e.printStackTrace()
    }
  }

  private[copytable] def runCopy(createFlag: Boolean)(implicit sc: SparkContext, runInfo: CopyTableInfo): Unit = {
    val fields: List[String] = runInfo.projectFields.getOrElse("").split(",").toList
    val dbRDD = sc.loadFromMapRDB(runInfo.source.get)
      .select(fields:_*)
    val filterRDD = buildPredicate match {
      case Some(p) => dbRDD.where(p)
      case None => dbRDD
    }
    filterRDD.saveToMapRDB(createTable = createFlag, tablename = runInfo.sink.get, bulkInsert = true)
  }

  private[copytable] def buildPredicate(implicit runInfo: CopyTableInfo): Option[Predicate] = {
    (runInfo.startKey, runInfo.endKey) match {
      case (Some(x), Some(y)) => Some(field("_id") between (runInfo.startKey.get, runInfo.endKey.get))
      case (Some(x), None) => Some(field("_id") >= x)
      case (None, Some(y)) => Some(field("_id") <= y)
      case (None, None) => None
    }
  }

  private[copytable] def parseArgs(args: Array[String]): CopyTableInfo = {
    var src: Option[String] = None
    var sink: Option[String] = None
    var startKey: Option[String] = None
    var endKey: Option[String] = None
    var projectFields: Option[String] = None
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
          case "-startkey" => {
            startKey = {
            if(args.isDefinedAt(args.indexOf(value)+1))
              Some(args(args.indexOf(value)+1))
            else
              None
            }
            if(startKey.isEmpty) {
              println("[ERROR]: Value for -startkey is missing")
              usage()
            }
          }
          case "-endkey" => {
            endKey = {
            if(args.isDefinedAt(args.indexOf(value)+1))
              Some(args(args.indexOf(value)+1))
            else
              None
            }
            if(endKey.isEmpty) {
              println("[ERROR]: Value for -endkey is missing")
              usage()
            }
          }
          case "-fields" => {
            projectFields = {
              if(args.isDefinedAt(args.indexOf(value)+1))
                Some(args(args.indexOf(value)+1))
              else
                None
            }
            if(projectFields.isEmpty) {
              println("[ERROR]: Value for -fields is missing")
              usage()
            }
          }
          case "-h" | "-help" => usage()
          case _ => if(value.startsWith("-")) {
            println(s"[ERROR] - Unrecognized argument: $value")
            usage()
          }
        }
    }
    //Validate arguments
    if(src.isEmpty || sink.isEmpty) {
      println("[ERROR] - Mandatory arguments not provided")
      usage()
    }

    if(startKey.isDefined && endKey.isDefined) {
      if(startKey.get > endKey.get)
        return CopyTableInfo(src, sink, endKey, startKey, projectFields)
    }
    CopyTableInfo(src, sink, startKey, endKey, projectFields)
  }

  private[copytable] def usage(): Unit = {
    println(s"Usage: $appName [Options] -src <Input text file/directory path> -sink <MapRDB-JSON sink table path>")
    println(s"Options:")
    println(s"-h or -help <For usage> ")
    println(s"-startkey <Start key>")
    println(s"-endkey <End key>")
    println(s"-fields <Comma separated list of fieldpaths [Ex: a.b, c.d.e etc]>")
    System.exit(1)
  }

  /**
    * Sets up the destination / sink table.
    * If table exists, it is not touched. Else, create sink table
    * with same metadata as source.
    * @param srcPath - Path to the source table, which will be used as reference.
    * @param sinkPath - Path to the sink / destination table.
    * @throws java.io.IOException - Throws DBExceptions.
    * @return - Returns true if sink table was created, else false.
    */
  @throws(classOf[java.io.IOException])
  private [copytable] def setupSinkTable(srcPath: String, sinkPath: String): Boolean = {
    val admin: Admin = MapRDB.newAdmin()
    try {
      !admin.tableExists(sinkPath)
    } catch {
      case e: Exception => {
        throw new java.io.IOException(e.getMessage, e.getCause)
      }
    } finally {
      admin.close()
    }
  }
}

