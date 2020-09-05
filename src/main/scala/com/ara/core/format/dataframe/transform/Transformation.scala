package com.ara.core.format.dataframe.transform

import scala.collection.JavaConverters._
import org.apache.spark.sql.{Column, DataFrame}
import com.typesafe.config.Config
import org.apache.spark.sql.functions.{col, expr, when}
import org.apache.spark.sql.types.StructField

class Transformation {

  def addConditionalLiteralColumnDataFrame(inputDF: DataFrame, conditionLitConfigList: List[Config]):DataFrame = {
    if(conditionLitConfigList.nonEmpty){
      conditionLitConfigList.foldLeft(inputDF) {
        case (df, conditionalLiteralConfig) =>
          val columnName = conditionalLiteralConfig.getString("column")
          val filterList = conditionalLiteralConfig.getConfigList("filters").asScala.toList
          val filterListHead :: filterListTail = filterList
          val headFilterCondition = filterListHead.getString("condition")
          val headFilterValue = filterListHead.getString("value")
          val headConditionColumn = when(expr(headFilterCondition), headFilterValue)
          val finalConditionColumn = if(filterListTail.nonEmpty){
            filterListTail.foldLeft(headConditionColumn) {
              case (conditionColumn, filter) =>
                val litCondition = filter.getString("condition")
                val litValue = filter.getString("value")
                conditionColumn.when(expr(litCondition), litValue)
            }
          } else {
            headConditionColumn
          }

          if (conditionalLiteralConfig.hasPath("defaultValue") && conditionalLiteralConfig.hasPath("defaultColumn")) {
            throw new IllegalArgumentException(s"Conditional Literal expects" +
            s" either default column or default value")
          }

          if (conditionalLiteralConfig.hasPath("defaultValue")) {
            val defaultValue = conditionalLiteralConfig.getString("defaultValue")
            df.withColumn(columnName, finalConditionColumn.otherwise(defaultValue))
          } else if (conditionalLiteralConfig.hasPath("defaultColumn")) {
            val defaultColumn = conditionalLiteralConfig.getString("defaultColumn")
            df.withColumn(columnName, finalConditionColumn.otherwise(col(defaultColumn)))
          } else {
            throw new IllegalArgumentException (s"Conditional Literal expects either default column or default value")
          }

      }
    } else {
      inputDF
    }
  }
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
