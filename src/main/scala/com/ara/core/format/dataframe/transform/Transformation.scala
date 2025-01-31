package com.ara.core.format.dataframe.transform

import java.sql.Date
import java.text.SimpleDateFormat

import scala.collection.JavaConverters._
import org.apache.spark.sql.{Column, DataFrame}
import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

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
  def getFilteredDataFrame(inputDF: DataFrame, filterConfigList: List[Config]):DataFrame = {
    if (filterConfigList.isEmpty) {
      inputDF
    } else {
      val filterQuery = filterConfigList.map{ config =>
        val operator =  config.getString("operator")
        val leftColumnName = config.getString("left_column")
        val columnStructField = inputDF.schema.find(structField=>
          structField.name.toLowerCase() ==leftColumnName.toLowerCase).getOrElse(
          throw new IllegalArgumentException(s"${getClass.getSimpleName} $leftColumnName. is not present in dataframe")
        )

        val dfCol = if (config.hasPath("substr")) {
          val subs = config.getIntList("substr").asScala.toList
          inputDF(leftColumnName).substr(subs.head, subs(1))
        }
        else {
          inputDF(leftColumnName)
        }

        val x = if(config.hasPath("valueList")){
          lazy val valueList = config.getStringList("valueList").asScala.toList.map(castValueDataType(columnStructField, _))
         operator match {
            case "not in" => !dfCol.isin(valueList: _*)
            case "in" =>  dfCol.isin(valueList: _*)
            case _ => throw new IllegalArgumentException(s"${getClass.getSimpleName} $operator. is not supported")
          }
        }
        else {
          val castValue = if(config.hasPath("value")){
            val value = config.getString("value")
            castValueDataType(columnStructField, value)
          }
          else {
            val rightColumn = config.getString("right_column")
            inputDF(rightColumn)
          }

          operator match {
            case ">" => dfCol.gt(castValue)
            case ">=" => dfCol.geq(castValue)
            case "<" => dfCol.lt(castValue)
            case "<=" => dfCol.leq(castValue)
            case "=" | "=="  => dfCol === castValue
            case "!=" | "=!=" | "<>" => dfCol.notEqual(castValue)
            case "<=>" => dfCol <=> castValue
            case "!<=>" => dfCol.isNull || (dfCol =!= castValue)
            case "not null" => dfCol.isNotNull
            case "like" => dfCol.like(castValue.asInstanceOf[String])
            case _ => throw new IllegalArgumentException(s"${getClass.getSimpleName} $operator. is not supported")

          }
        }
       x
      }.reduce(_ && _)
      inputDF.filter(filterQuery)
    }
  }
  private def castValueDataType(columnStructField: StructField, value: String):Any = {
    val dataType = columnStructField.dataType
    try {
      dataType match {
        case DateType => Date.valueOf(value)
        case BooleanType => value.toBoolean
        case FloatType => value.toFloat
        case LongType => value.toLong
        case DoubleType => value.toDouble
        case IntegerType => value.toInt
        case StringType => String.valueOf(value)
        }
    }
    catch {
      case _ : MatchError => throw new IllegalArgumentException(s"${getClass.getSimpleName} $dataType. is not supported")
      case _ : Throwable => throw new IllegalArgumentException(s"${getClass.getSimpleName} $dataType. " +
      s"Type of ${columnStructField.name} and $value does not match")
    }
  }

  /**
   *
   * @param inputDF  DataFrame to add Fork Column
   * @param forkConfig Json of Type
   *                   { "columnA", "columnB",
   *                   "columnC", "columnD"
   *                   }
   * @return output DataFrame with Forked Column
   */
  def forkColumnDataFrame(inputDF: DataFrame, forkConfig: Config):DataFrame = {
    if(forkConfig.isEmpty){
      inputDF
    } else {
      val columnName = forkConfig.root().entrySet().asScala.map(_.getKey)
      val outputDF = columnName.foldLeft(inputDF){ (df,col)=>
        df.withColumn(col, df(forkConfig.getString(col)))
      }
      outputDF
    }

  }
  def lowerCaseColumn(inputDF: DataFrame, toLowerCaseConfig: Config):DataFrame = {
    if(toLowerCaseConfig.getBoolean("toLowerCase")) {
      val columnProjection = inputDF.schema.fields.map{ col=>
        inputDF.col(col.name).as(col.name.toLowerCase())
      }
      inputDF.select(columnProjection: _*)
    } else { inputDF }
  }

  def modifyDatePattern(inputDF: DataFrame, config: Config):DataFrame = {

    val columns = config.root().keySet().asScala.toList
    val modifyDatePatternUDF = udf((time: String, readPattern: String, writePattern: String)=>{

      if (time ==null || time =="" || time=="null") {
        time
      } else {
        val readFormat = new SimpleDateFormat(readPattern)
        val writeFormat = new SimpleDateFormat(writePattern)
        val readDate = readFormat.parse(time)
        val outputRunTimeString = writeFormat.format(readDate)
        outputRunTimeString
      }
    })

    columns.foldLeft(inputDF) { (df, colName) =>
      val columnConfig = config.getConfig(colName)
      val readPattern = columnConfig.getString("readPattern")
      val writePattern = columnConfig.getString("writePattern")
      df.withColumn(colName, modifyDatePatternUDF(df(colName), lit(readPattern), lit(writePattern)))
    }
  }

  /**
   *
   * @param inputDF Dataframe to do expression on select columns
   * @param expressionConfig Json of Type
   *                         { "column1": "cast(amount/10000000000 as DECIMAL(8,6))",
   *                         "column2": "2"
   *                         }
   * @return DataFrame
   */
  def addExpressionColumnDatePattern(inputDF: DataFrame, expressionConfig: Config):DataFrame = {
    if (expressionConfig.isEmpty) {
      inputDF
    } else {
      val columnNameList = expressionConfig.root().entrySet().asScala.toList.map(_.getKey)
      val outputDF = columnNameList.foldLeft(inputDF){(df, col) =>
        df.withColumn(col, exp(expressionConfig.getString(col)))
      }
      outputDF.sqlContext.createDataFrame(outputDF.rdd, StructType(outputDF.schema.map(_.copy(nullable = true))))
    }

  }

}
object Transformation{
  def apply(): Transformation = new Transformation()
}
