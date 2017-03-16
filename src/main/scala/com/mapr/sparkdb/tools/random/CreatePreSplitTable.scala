package com.mapr.sparkdb.tools.random

import org.apache.spark.{SparkConf, SparkContext}
import com.mapr.db.spark._
import com.mapr.db._
import com.mapr.sparkdb.tools.common.SparkToolsConstants._
import com.typesafe.config._
import org.ojai.Value
import com.mapr.db.impl.{AdminImpl, IdCodec, TabletInfoImpl}
import com.mapr.db.spark.condition.Predicate
import com.mapr.sparkdb.tools.common.{CopyTableInfo, Utils}

object CreatePreSplitTable {
  val appName = "CreatePreSplitTable"

  def main(args: Array[String]): Unit = {
    try {
      //Parse arguments
      implicit val runInfo = parseArgs(args)
      println("Source File: " + runInfo.source.getOrElse(""))
      println("Sink Table: " + runInfo.sink.getOrElse(""))

      setupSinkTable(runInfo.source.get, runInfo.sink.get)
    } catch {
      case e: Throwable => e.printStackTrace()
    }
  }

  private[random] def parseArgs(args: Array[String]): CopyTableInfo = {
    var src: Option[String] = None
    var sink: Option[String] = None
    var startKey: Option[String] = None
    var endKey: Option[String] = None
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
          case _ => if(value.startsWith("-"))
            println(s"[WARN] - Unrecognized argument $value")
        }
    }
    //Validate arguments
    if(src.isEmpty || sink.isEmpty) {
      usage()
    }
    CopyTableInfo(src, sink, startKey, endKey)
  }

  private[random] def usage(): Unit = {
    println(s"Usage: $appName -src <Input text file/directory path> -sink <MapRDB-JSON sink table path> [Options]")
    println(s"Options:")
    println(s"-startkey <Start key>")
    println(s"-endkey <End key>")
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
  private [random] def setupSinkTable(srcPath: String, sinkPath: String): Unit = {
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
      }
    } catch {
      case e: Exception => {
        throw new java.io.IOException(e.getMessage, e.getCause)
      }
    } finally {
      admin.close()
    }
  }
}

