package com.ara.core.format.dataframe

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SQLContext}

trait DataFrameReadWriteFormat {

 val config: Config

  def readDataFrame(sqlContext: SQLContext): DataFrame
  def writeDataFrame(df: DataFrame): Unit
  def formatDataFrame(df: DataFrame): DataFrame
  def addHeader
}
