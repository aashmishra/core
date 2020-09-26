package com.ara.core.format.dataframe.table

import com.ara.core.format.dataframe.file.FileDataFrameReadWriteFormat
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SQLContext}

class HiveTableReadWriteFormat(_config: Config) extends FileDataFrameReadWriteFormat{

  override val config: Config = _config

  override def readDataFrame(sqlContext: SQLContext): DataFrame = ???

  override def writeDataFrame(df: DataFrame): Unit = ???

}
