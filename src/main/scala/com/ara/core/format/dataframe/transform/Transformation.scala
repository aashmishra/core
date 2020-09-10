package com.ara.core.format.dataframe.transform

import scala.collection.JavaConverters._
import org.apache.spark.sql.{Column, DataFrame}
import com.typesafe.config.Config
import org.apache.spark.sql.functions.{col, column, expr, lit, when}
import org.apache.spark.sql.types.{StructField, StructType}

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

  /**
   *  drops the duplicate
   * @param inputDF
   * @param dropDuplicateConfig  { "columns": ["col1","col2","col3"]
   *                             }
   * @return
   */
  def dropDuplicateColumnDataFrame(inputDF: DataFrame, dropDuplicateConfig: Config):DataFrame = {
  if(dropDuplicateConfig.isEmpty){ inputDF }
  else {
    val columnList = dropDuplicateConfig.getStringList("columns").asScala
    if(columnList.isEmpty) { inputDF.dropDuplicates() }
    else { inputDF.dropDuplicates(columnList) }
  }
  }

  /**
   *
   * @param inputDF
   * @param literalConfig Json of Type
   *                      ''{
   *                      "column1":"someLit",
   *                      "column2":"someotherLit"
   *                      }''
   * @return DataFrame
   */
  def addLiteralColumnDataFrame(inputDF: DataFrame, literalConfig: Config):DataFrame = {
    if(literalConfig.isEmpty){
      inputDF
    }
    else {
      val columnNameList = literalConfig.root().entrySet().asScala.map(_.getKey)
      val outputDF = columnNameList.foldLeft(inputDF){(df,col)=>
        df.withColumn(col, lit(literalConfig.getAnyRef(col)))
      }
      inputDF.sqlContext.createDataFrame(outputDF.rdd, StructType(outputDF.schema.map(_.copy(nullable = true))))
    }
  }

  /**
   * Casts the columns of the DataFrame according to the casting config provided
   *
   * @param inputDF
   * @param castConfig {"column1": ["String","Date"], "column2": ["String","Int"] }
   * @return
   */
  def castColumnDataFrame(inputDF: DataFrame, castConfig: Config):DataFrame = {
    if  (castConfig.isEmpty){
      inputDF
    } else {
      val columnNameList = castConfig.root().entrySet().asScala.map(_.getKey).toSeq
      val projections = inputDF.dtypes.map{ case (colName, colType) =>
        if(columnNameList.contains(colName)){
          val castingOrder = castConfig.getStringList(colName).asScala.toList
          val updatedCastingList = castingOrder.map(_.toLowerCase())
          castColumn(inputDF(colName), colType, updatedCastingList).alias(colName)
        }
        else{
          inputDF(colName)
        }
      }
      inputDF.select(projections : _*)
    }
  }

  /**
   * Cast a column in the order of casting list and return a column
   * @param column
   * @param colType
   * @param castList List["string", "double"]
   * @return
   */
  def castColumn(column: Column, colType: String, castList: List[String]):Column = {
    castList.foldLeft(column){ (col, castType)=>
      if(colType == "StringType" && castType == "boolean") {
        when(col === "true", true).otherwise(false)
      } else {
        col.cast(castType)
      }
    }

  }

  /**
   * Rename dataFrame column names
   *
   * @param inputDF
   * @param columnRenameConfig rename config of Json Type
   *                           {
   *                           col1: col2,
   *                           col4: col6
   *                           }
   * @return
   */
  def columnRenameDataFrame(inputDF: DataFrame, columnRenameConfig: Config):DataFrame = {
    val columnProjects = inputDF.schema.fields.map{ col =>
      if(columnRenameConfig.hasPath(col.name)){
        inputDF.col(col.name).alias(columnRenameConfig.getString(col.name))
      }
      else { inputDF.col(col.name) }
    }
    inputDF.select(columnProjects: _*)
  }

  /**
   * Get Filter DataFrame based on Config
   *
   * @param inputDF
   * @param filterConfigList
   * @return
   */
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
