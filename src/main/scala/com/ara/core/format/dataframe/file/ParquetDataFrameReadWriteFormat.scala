package com.ara.core.format.dataframe.file

import com.typesafe.config.Config
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.JavaConverters._

class ParquetDataFrameReadWriteFormat(_config: Config) extends FileDataFrameReadWriteFormat
{

  override val config: Config = _config

  override def readDataFrame(sqlContext: SQLContext): DataFrame = {

    val optionConfig = config.getConfig("option")
    val filePath = optionConfig.getString("path")
    val resourceFile = if(optionConfig.hasPath("resource")) {
      optionConfig.getBoolean("resource")
    } else {
      false
    }
    val updatedFilePath = if(resourceFile){
      getClass.getResource(filePath).getFile
    } else {
      filePath
    }
//    val optionMap = optionConfig.entrySet().asScala.toList.map{
//      entry => (entry.getKey, entry.getValue.unwrapped().toString)
//    }.toMap

    val readDF = sqlContext.read.parquet(updatedFilePath)

    val formattedDF = formatDataFrame(readDF)
    val selectedColumnsDF = if(optionConfig.hasPath("columnNames") && !optionConfig.getStringList("columnNames").isEmpty) {
      val columnNameList = optionConfig.getStringList("columnNames").asScala.toList
      addHeader(formattedDF.select(columnNameList.head, columnNameList.tail: _*))
    } else {
      addHeader(formattedDF)
    }

    if(optionConfig.hasPath("limit")){
      val limitRead = optionConfig.getInt("limit")
      selectedColumnsDF.limit(limitRead)
    } else {
      selectedColumnsDF
    }

  }

  override def writeDataFrame(inputDF: DataFrame): Unit = {
    val formatDF = formatDataFrame(inputDF)
    val optionConfig = config.getConfig("option")

    val  writeDF = if(optionConfig.hasPath("columnName")){
      val writeColumns = optionConfig.getStringList("columnNames").asScala.toList
      formatDF.select(writeColumns.map(col):_*)

    } else {
      formatDF
    }
    val filePath = optionConfig.getString("path")

//    val optionMap = optionConfig.entrySet().asScala.toList.map{
//      entry => (entry.getKey, entry.getValue.unwrapped().toString)
//    }.toMap

    val saveModeString = if(optionConfig.hasPath("saveMode")){
      optionConfig.getString("saveMode").toLowerCase
    }  else { "errorifexists" }

    val saveMode = getSaveMode(saveModeString)

    val coalesceWriteDF = if(optionConfig.hasPath("coalesce")){
      writeDF.coalesce(optionConfig.getInt("coalesce"))
    } else {
      writeDF
    }

    val repartitionDf = if (optionConfig.hasPath("repartition")) {
      val repartitionConfig = optionConfig.getConfig("repartition")
      val repartitionColumns = if (repartitionConfig.hasPath("repartitionColumns")) {
        repartitionConfig.getStringList("repartitionColumns").asScala.toList

      } else { Nil }

      if(repartitionConfig.hasPath("repartitionNumber")) {
        val repartitionNumber = repartitionConfig.getInt("repartitionNumber")
        repartitionColumns match {
          case Nil => coalesceWriteDF.repartition(repartitionNumber)
          case _ => coalesceWriteDF.repartition(repartitionNumber, repartitionColumns.map(col): _*)
        }
      } else {
        repartitionColumns match {
          case Nil => coalesceWriteDF
          case _ => coalesceWriteDF.repartition(repartitionColumns.map(col): _*)
        }
      }


    } else {
      coalesceWriteDF
    }
    val sortedDF = if(optionConfig.hasPath("sortWithinPartitions")) {
      val orderByColConfig = optionConfig.getConfigList("sortWithinPartitions").asScala.toList
      val orderByColMap = orderByColConfig.map{
        conf => (conf.getString("colName"), conf.getString("sortOrder"))
      }.filter(_._1.nonEmpty)

      val orderBy = orderByColMap.map{
        case (x, y) => y match {
          case sortBy if sortBy.equalsIgnoreCase("desc") => col(x).desc_nulls_last
          case _ => col(x).asc_nulls_last
        }

      }
      repartitionDf.sortWithinPartitions(orderBy: _*)
    } else {
      repartitionDf
    }

    val partitions = if (optionConfig.hasPath("partitionBy")) {
      optionConfig.getStringList("partitionBy").asScala.toList
    } else {
      List.empty
    }

    sortedDF.write
      .mode(saveMode)
      .partitionBy(partitions: _*)
      .parquet(filePath)
  }

}


