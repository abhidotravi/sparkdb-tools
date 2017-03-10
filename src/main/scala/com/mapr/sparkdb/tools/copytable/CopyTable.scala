package com.mapr.sparkdb.tools.copytable

import org.apache.spark.{SparkConf, SparkContext}
import com.mapr.db.spark._
import com.mapr.db._
import com.mapr.sparkdb.tools.common.SparkToolsConstants._
import com.typesafe.config._
import org.ojai.Value
import com.mapr.db.impl.{AdminImpl, IdCodec, TabletInfoImpl}
import com.mapr.sparkdb.tools.common.{RunnerInfo, Utils}

object CopyTable {
  val appName = "CopyTable"

  def main(args: Array[String]): Unit = {
    try {
      //Parse arguments
      implicit val runInfo = parseArgs(args)
      println("Source File: " + runInfo.source)
      println("Sink Table: " + runInfo.sink)

      //Load config
      val config: Config = ConfigFactory.load("application")
      val appName: String = "CopyTable"
      val conf: SparkConf = new SparkConf()
        .setAppName(appName)
        .setSparkHome(config.getString(SPARK_HOME_CONFIG))
        .set(SERIALIZER_CONFIG, config.getString(SERIALIZER_CONFIG))
        .set(KRYO_REGISTRATOR_CONFIG, config.getString(KRYO_REGISTRATOR_CONFIG))
      implicit val sc: SparkContext = SparkContext.getOrCreate(conf)

      println("SparkContext config: " + sc.getConf.toDebugString)

      //Switch bulkload on source to false
      Utils.unsetBulkLoad(runInfo.source)
      val isNew: Boolean = setupSinkTable(runInfo.source, runInfo.sink)

      try {
        runCopy
      } catch {
        case e: Throwable => if(isNew) Utils.unsetBulkLoad(runInfo.sink)
      }

      //Do not modify bulkload if table already existed
      if (isNew) Utils.unsetBulkLoad(runInfo.sink)
    } catch {
      case e: Throwable => e.printStackTrace()
    }
  }

  def runCopy(implicit sc: SparkContext, runInfo: RunnerInfo): Unit = {
    sc.loadFromMapRDB(runInfo.source)
      .saveToMapRDB(createTable = false, tablename = runInfo.sink, bulkInsert = true)
  }

  private[copytable] def parseArgs(args: Array[String]): RunnerInfo = {
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

  private[copytable] def usage(): Unit = {
    println(s"Usage: $appName -src <Input text file/directory path> -sink <MapRDB-JSON sink table path>")
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
      if (!admin.tableExists(sinkPath)) {
        val tableDesc: TableDescriptor = admin.getTableDescriptor(srcPath)
          .setPath(sinkPath)
          .setBulkLoad(true)

        val srcTable: Table = MapRDB.getTable(srcPath)
        val splits: Array[Value] = MapRDB
          .getTable(srcPath)
          .getTabletInfos
          .map(x => x.asInstanceOf[TabletInfoImpl])
          .map(x => IdCodec.decode(x.getStartRow))

        admin
          .asInstanceOf[AdminImpl]
          .createTable(tableDesc, splits.drop(1))
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

