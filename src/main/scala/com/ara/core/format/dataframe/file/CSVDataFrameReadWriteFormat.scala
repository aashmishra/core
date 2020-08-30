package com.ara.core.format.dataframe.file

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SQLContext}
class CSVDataFrameReadWriteFormat(_config: Config) extends FileDataFrameReadWriteFormat {


  override val config: Config = _config

  override def readDataFrame(sqlContext: SQLContext): DataFrame = ???

  override def writeDataFrame(df: DataFrame): Unit = ???
}

