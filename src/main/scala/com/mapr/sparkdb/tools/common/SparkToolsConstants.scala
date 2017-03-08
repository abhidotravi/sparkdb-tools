package com.mapr.sparkdb.tools.common

/**
  * Created by aravi on 2/28/17.
  * Constants used by Spark Tools.
  */

object SparkToolsConstants {
  val SPARK_HOME_CONFIG = "spark.home"
  val EXECUTOR_MEMORY_CONFIG = "spark.executor.memory"
  val DRIVER_MEMORY_CONFIG = "spark.driver.memory"
  val SERIALIZER_CONFIG = "spark.serializer"
  val KRYO_REGISTRATOR_CONFIG = "spark.kryo.registrator"
  val EXECUTOR_JAVA_OPTS_CONFIG = "spark.executor.extraJavaOptions"
  val DRIVER_JAVA_OPTS_CONFIG = "spark.driver.extraJavaOptions"

  //Hadoop Configuration related constants
  val HADOOP2_VERSION = "2.7.0"
  val HADOOP_HOME: String = "/opt/mapr/hadoop/hadoop-" + HADOOP2_VERSION + "/"
  val HADOOP_CONF_DIR: String = HADOOP_HOME + "/etc/hadoop/"
  val HADOOP_MAPRED_SITE: String = HADOOP_CONF_DIR + "mapred-site.xml"
  val HADOOP_YARN_SITE: String = HADOOP_CONF_DIR + "yarn-site.xml"
}
