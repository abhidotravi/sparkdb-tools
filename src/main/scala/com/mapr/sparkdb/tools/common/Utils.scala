package com.mapr.sparkdb.tools.common

import org.apache.spark.{SparkConf, SparkContext}
import SparkToolsConstants._
import com.mapr.db.{Admin, MapRDB, TableDescriptor}
import com.typesafe.config._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.launcher.SparkLauncher

/**
  * Created by aravi on 2/28/17.
  */
object Utils {
  /**
    * Sets "bulkload" property to false.
    * @param path - Table path
    * @throws java.io.IOException - Throws DBExceptions.
    */
  @throws(classOf[java.io.IOException])
  def unsetBulkLoad(path: String): Unit = {
    val admin: Admin = MapRDB.newAdmin()
    try {
      if (!admin.tableExists(path)) {
        println("ERROR: Source table does not exist")
        admin.close()
        System.exit(-1)
      }
      val descriptor: TableDescriptor = admin.getTableDescriptor(path)
      descriptor.setBulkLoad(false)
      admin.alterTable(descriptor)
    } catch {
      case e: Exception => {
        throw new java.io.IOException(e.getMessage, e.getCause)
      }
    } finally {
      admin.close()
    }
  }

  @throws(classOf[java.lang.IllegalArgumentException])
  def createHadoopConfiguration(isHadoop2: Boolean = true): Configuration = {
    val conf: Configuration = new Configuration()
    if(isHadoop2) {
      conf.addResource(HADOOP_MAPRED_SITE)
      conf.addResource(HADOOP_YARN_SITE)
    } else {
      println("WARN: HADOOP1 is not supported")
      conf.set("fs.default.name", "maprfs:///")
    }
    conf
  }
}
