package com.ara.core.format.dataframe.transform

import org.apache.spark.sql.{Column, DataFrame}
import com.typesafe.config.Config
import org.apache.spark.sql.types.StructField

class Transformation {

  def addConditionalLiteralColumnDataFrame(inputDF: DataFrame, conditionLitConfigList: List[Config]):DataFrame = ???
  def dropDuplicateColumnDataFrame(inputDF: DataFrame, dropDuplicateConfig: Config):DataFrame = ???
  def addLiteralColumnDataFrame(inputDF: DataFrame, literalConfig: Config):DataFrame = ???
  def castColumnDataFrame(inputDF: DataFrame, literalConfig: Config):DataFrame = ???
  def castColumn(column: Column, colType: String, castList: List[String]):Column = ???
  def columnRenameDataFrame(inputDF: DataFrame, columnRenameConfig: Config):DataFrame = ???
  def getFilteredDataFrame(inputDF: DataFrame, filterConfigList: List[Config]):DataFrame = ???
  def castValueDataType(columnStructField: StructField, value: String):DataFrame = ???
  def forkColumnDataFrame(inputDF: DataFrame, forkConfig: Config):DataFrame = ???
  def lowerCaseColumn(inputDF: DataFrame, toLowerCaseFlag: Boolean):DataFrame = ???
  def modifyDatePattern(inputDF: DataFrame, config: Config):DataFrame = ???
  def addExpressionColumnDatePattern(inputDF: DataFrame, expressionConfig: Config):DataFrame = ???
}
object Transformation{
  def apply(): Transformation = new Transformation()
}
