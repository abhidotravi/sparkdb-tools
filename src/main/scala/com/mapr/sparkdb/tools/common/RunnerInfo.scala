package com.mapr.sparkdb.tools.common

/**
  * Created by aravi on 3/9/17.
  */
private[tools] abstract class RunnerInfo
private[tools] case class CopyTableInfo(source: Option[String],
                                        sink: Option[String],
                                        startKey: Option[String],
                                        endKey: Option[String],
                                        projectFields: Option[String])
private[tools] case class ExportJsonInfo(source: Option[String],
                                         sink: Option[String])
private[tools] case class ImportJsonInfo(source: Option[String],
                                         sink: Option[String],
                                         id: Option[String])
