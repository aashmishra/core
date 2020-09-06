package com.ara.core.format.dataframe.transform

import java.sql.Date

import scala.collection.JavaConverters._

import org.apache.spark.sql.Row
import com.ara.core.unittest.sharedcontext.SharedSparkSession
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite
import com.ara.core.unittest.dataframe.DataFrameUtility.assertDataFrame

class TransformationTest extends FunSuite with SharedSparkSession {

  test("TransformationTest - Add Literal Column") {
    val inputRDD = sc.parallelize(
      List(
        Row.fromTuple("AD234", "kiran", "sahiba", 20, Date.valueOf("2020-09-06"))
      )
    )

    val inputDataFrameSchema = StructType(
      Array(
        StructField("id", StringType),
        StructField("firstname", StringType),
        StructField("lastname", StringType),
        StructField("age", IntegerType),
        StructField("dob", DateType)
    ))
    val inputDF = sqlContext.createDataFrame(inputRDD, inputDataFrameSchema)
    val literalConfig = List.empty[Config]
    val actualDF = Transformation().addConditionalLiteralColumnDataFrame(inputDF, literalConfig)
    actualDF.show(20, false)
    assertDataFrame(actualDF, inputDF)
  }
}
