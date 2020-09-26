package com.ara.core.unittest.dataframe

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{StructField, StructType}

object DataFrameUtility {

  def assertDataFrame(xDF: DataFrame, yDF: DataFrame):Unit={
    val xColumnNameList = xDF.columns.toList
    val yColumnNameList = yDF.columns.toList

    assert(xColumnNameList.forall(x=> yColumnNameList.contains(x)),"Column Name do Not match - XDF: "
    + xColumnNameList.mkString(",") + " - yDF : " + yColumnNameList.mkString(","))
    assert(yColumnNameList.forall(y=> xColumnNameList.contains(y)),"Column Name do Not match - yDF: "
      + yColumnNameList.mkString(",") + " - xDF : " + xColumnNameList.mkString(","))

    val selectYDF = yDF.select(xColumnNameList.head, xColumnNameList.tail: _*)
    val selectXDF = xDF.select(yColumnNameList.head, yColumnNameList.tail: _*)

    //comparing schema of both the dataframe

    val nullableSetXDF = setNullableForAllColumns(selectXDF)
    val nullableSetYDF = setNullableForAllColumns(selectYDF)

    val xDFSchema = nullableSetXDF.schema
    val yDFSchema = nullableSetYDF.schema

    assert(xDFSchema.diff(yDFSchema).lengthCompare(0) == 0, "xDF schema does not match yDF schema")

    //comparing Content of both the DataFrame.

    if(xDF.except(selectYDF).count() !=0){
      xDF.show(20,  false)
      selectYDF.show(20,  false)
      throw new AssertionError("XDF content does not match YDF content")
    }

    if(yDF.except(selectXDF).count() !=0){
      yDF.show(20,  false)
      selectXDF.show(20,  false)
      throw new AssertionError("YDF content does not match XDF content")
    }
  }



  private def setNullableForAllColumns(df: DataFrame, nullable: Boolean = true) : DataFrame = {
    df.sqlContext.createDataFrame(df.rdd, StructType(df.schema.map(_.copy(nullable = nullable))))
  }

  def unionDataFrame(xDF: DataFrame, yDF: DataFrame): DataFrame = {
    val xDFSchema = xDF.schema
    val yDFSchema = yDF.schema

    val combinedSchema = (xDFSchema ++ yDFSchema).distinct
    val combinedColumns = (xDF.columns ++ yDF.columns).distinct

    val combinedSchemaNullableInferred = combinedSchema
      .map(schema=>(schema.name, schema.dataType)).distinct

    if(combinedSchemaNullableInferred.length != combinedColumns.length){
      val combinedSchemaColumns = combinedSchemaNullableInferred.map(_._1).toList
      val columnWithTypeDifference = combinedSchemaColumns.diff(combinedColumns.toList)

      throw new IllegalStateException(s"${getClass.getName} - DataFrame cannot be unioned -" +
      s" conflicting dataType of Columns - ${columnWithTypeDifference.mkString(",")}")
    } else {
      val xDFDeltaSchema = combinedSchema.toSet.filter(x=> !xDF.columns.contains(x.name))
      val yDFDeltaSchema = combinedSchema.toSet.filter(y=> !yDF.columns.contains(y.name))

      val xDFComplete = addDeltaColumns(xDFDeltaSchema, combinedColumns, xDF)
      val yDFComplete = addDeltaColumns(yDFDeltaSchema, combinedColumns, yDF)
      xDFComplete.union(yDFComplete)
    }

  }


  private def addDeltaColumns(deltaSchema: Set[StructField], combineColumns: Array[String], initialDF: DataFrame): DataFrame={
    val outputDF = if (deltaSchema.nonEmpty) {
      deltaSchema.foldLeft(initialDF)((df, column)=> {
        df.withColumn(column.name, lit(null).cast(column.dataType).alias(column.name))
      })
    } else {
      initialDF
    }
    outputDF.select(combineColumns.head, combineColumns.tail: _*)
  }
}
