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
        Row.fromTuple("AD234", "kiran", "sahiba", 15, Date.valueOf("2005-09-06")),
        Row.fromTuple("AD223", "Issa", "lenin", 30, Date.valueOf("1990-09-06")),
        Row.fromTuple("AD294", "Jasmi", "xeba", 40, Date.valueOf("1980-09-06"))
      )
    )

    val expectedRDD = sc.parallelize(
      List(
        Row.fromTuple("AD234", "kiran", "sahiba", 15, Date.valueOf("2005-09-06"), "Can't Smoke"),
        Row.fromTuple("AD223", "Issa", "lenin", 30, Date.valueOf("1990-09-06"), "Allowed to smoke"),
        Row.fromTuple("AD294", "Jasmi", "xeba", 40, Date.valueOf("1980-09-06"), "Allowed to smoke")
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

    val expectedSchema = inputDataFrameSchema.add(StructField("category", StringType))
    val inputDF = sqlContext.createDataFrame(inputRDD, inputDataFrameSchema)
    val expectedDF = sqlContext.createDataFrame(expectedRDD, expectedSchema)
    val literalConfig = List.empty[Config]
val conf =
  """
    |"conditionalLit" :[
    |{"column" : "category",
    |"filters":[
    |{"condition": "age<18"
    |"value" : "Can't Smoke"}
    |],
    |"defaultValue":"Allowed to smoke"}]
    |""".stripMargin

    val configr = ConfigFactory.parseString(conf).getConfigList("conditionalLit").asScala.toList
    val actualDF = Transformation().addConditionalLiteralColumnDataFrame(inputDF, configr)
    actualDF.show(20, false)
    assertDataFrame(actualDF, expectedDF)
  }
}
