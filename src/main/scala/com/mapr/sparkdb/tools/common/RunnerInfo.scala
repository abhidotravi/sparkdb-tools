package com.mapr.sparkdb.tools.common

/**
  * Created by aravi on 3/9/17.
  */
private[tools] abstract class RunnerInfo
private[tools] case class CopyTableInfo(source: String, sink: String)
private[tools] case class ExportJsonInfo(source: String, sink: String)
private[tools] case class ImportJsonInfo(source: String, sink: String, id: String)
