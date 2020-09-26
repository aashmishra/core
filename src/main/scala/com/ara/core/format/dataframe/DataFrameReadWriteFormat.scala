package com.ara.core.format.dataframe

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import com.ara.core.format.dataframe.file.FileDataFrameReadWriteFormat
import com.ara.core.format.dataframe.table.HiveTableReadWriteFormat
import com.ara.core.format.dataframe.utils.helpers
import com.ara.core.format.dataframe.transform._
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.JavaConverters._

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

  //get the functions with config
  val doConditionalLiteral =  Transformation().addConditionalLiteralColumnDataFrame(_,helpers.checkAndGetConfList(config, "conditionalLiteral"))
  val doDropDuplicate = Transformation.apply().dropDuplicateColumnDataFrame(_, helpers.checkAndGetConf(config, "dropDuplicates"))
  val doRenameColumns = Transformation().columnRenameDataFrame(_, helpers.checkAndGetConf(config, "rename"))
  val doAddLiteral= Transformation().addLiteralColumnDataFrame(_, helpers.checkAndGetConf(config, "literal"))
  val doColumnCast= Transformation().castColumnDataFrame(_, helpers.checkAndGetConf(config, "cast"))
  val doModifyDatePattern = Transformation().modifyDatePattern(_, helpers.checkAndGetConf(config, "modifyDatePattern"))
  val doColumnFork = Transformation().forkColumnDataFrame(_, helpers.checkAndGetConf(config, "fork"))
  val applyFilter = Transformation().getFilteredDataFrame(_, helpers.checkAndGetConfList(config, "filterList"))
  val addColumnWithExpression = Transformation().addExpressionColumnDatePattern(_, helpers.checkAndGetConf(config, "expression"))
  val doColumnLowerCase = Transformation().lowerCaseColumn(_, helpers.checkAndGetConf(config, "toLowerCase"))


  //creating a List to define operations sequence
  val SequenceOperationList = List(
   doAddLiteral,
   doConditionalLiteral,
   addColumnWithExpression,
   doColumnCast,
   doColumnFork,
   doModifyDatePattern,
   applyFilter,
   doDropDuplicate,
   doRenameColumns,
   doColumnLowerCase)

  //Apply apply all functions
  SequenceOperationList.foldLeft(df){ (df, operate) => operate(df) }

 }

 def addHeader(dataFrame: DataFrame, config: Config):DataFrame={
  if(config.hasPath("headerList")) {
   val columnNameList: List[String] = config.getStringList("headerList").asScala.toList
   val dataFrameColumnList = dataFrame.columns.toList
   require(columnNameList.length==dataFrameColumnList.length, "Header column count is different from input file column count")

   val columnNameTupleList = dataFrameColumnList.zip(columnNameList)
   val columnList = columnNameTupleList.map {
    case (oldName, newName) => dataFrame(oldName).as(newName)
   }
   dataFrame.select(columnList: _*)
  } else {
  dataFrame}
 }

 def mergeSparkFiles(config: Config,spark: SparkSession):Unit = {

 }

 def renameSparkFiles(config: Config,spark: SparkSession):Unit = {

 }

 def createCompleteFile(path:String, fs:FileSystem,Suffix:String = ".complete"): Unit ={
  fs.createNewFile(new Path(path + Suffix))
 }
}
