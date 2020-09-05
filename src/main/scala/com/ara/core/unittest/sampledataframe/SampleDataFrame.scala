package com.ara.core.unittest.sampledataframe

import java.sql.Date

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
 * Return sample DataFrame
 */


object SampleDataFrame {

  def returnSampleDFType1(sqlContext: SQLContext):DataFrame = {
    sqlContext.createDataFrame(
      Seq(
        ("0","Agriculture"),
        ("1", "Mining"),
        ("2", "Construction"),
        ("3", "Manufacturing")
      )).toDF("code", "industry")

  }

  def returnSampleDFType2(sqlContext: SQLContext):DataFrame = {
    val innerRow1 = List(
      Row("RTS", "poi"),
      Row("asd","ASas")
    )

    val innerRow2 = List(
      Row("doctor", "442.98f"),
      Row("teacher","567.33f")
    )

    val innerRow3 = List(
      Row(innerRow2, "466")
    )

    val outerRow = List(
      Row("ID12345", Date.valueOf("2020-09-06"),innerRow1,innerRow3)
    )

    val row1Schema = StructType(
      Array(
      StructField("First_name", StringType),
      StructField("last_name", StringType)
    )
    )

  }

}
