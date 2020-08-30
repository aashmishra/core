package com.ara.core.format.dataframe

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import com.ara.core.format.dataframe.file.FileDataFrameReadWriteFormat
import com.ara.core.format.dataframe.table.HiveTableReadWriteFormat

trait DataFrameReadWriteFormat {

 val config: Config

  def readDataFrame(sqlContext: SQLContext): DataFrame
  def writeDataFrame(df: DataFrame): Unit
  def formatDataFrame(df: DataFrame): DataFrame=DataFrameReadWriteFormat.formatDataFrame(df, config)
  def addHeader(df: DataFrame):DataFrame=DataFrameReadWriteFormat.addHeader(df, config)
 def mergeSparkFiles(spark: SparkSession)=DataFrameReadWriteFormat.mergeSparkFiles(config, spark)
 def renameSparkFiles(spark: SparkSession)=DataFrameReadWriteFormat.renameSparkFiles(config, spark)
}

object DataFrameReadWriteFormat{
 def apply(config: Config): DataFrameReadWriteFormat = {
  val formatType = config.getString("type").toLowerCase
  formatType match {
   case s if s.startsWith("file")=>FileDataFrameReadWriteFormat(config)
   case "table.hive" => new HiveTableReadWriteFormat(config)
   case _ => throw new IllegalArgumentException(
    "DataFrameReadWriteFormat does not support specified format type: " + formatType
   )
  }
 }

 def formatDataFrame(df: DataFrame, config: Config):DataFrame={
  df
 }

 def addHeader(df: DataFrame, config: Config):DataFrame={
  df
 }

 def mergeSparkFiles(config: Config,spark: SparkSession):Unit = {

 }

 def renameSparkFiles(config: Config,spark: SparkSession):Unit = {

 }
}
