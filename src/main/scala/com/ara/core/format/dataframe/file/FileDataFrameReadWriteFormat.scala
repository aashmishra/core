package com.ara.core.format.dataframe.file

import com.typesafe.config.Config
import com.ara.core.format.dataframe.DataFrameReadWriteFormat
import org.apache.spark.sql.SaveMode

trait FileDataFrameReadWriteFormat extends DataFrameReadWriteFormat{

  protected def getSaveMode(saveModeString: String):SaveMode = {
    saveModeString match {
      case "overwrite" => SaveMode.Overwrite
      case "append" => SaveMode.Append
      case "ignore" => SaveMode.Ignore
      case "errorifexists" => SaveMode.ErrorIfExists
      case _ => throw new IllegalArgumentException("File Does not support specific SaveMode option : "+ saveModeString)
    }
  }
}

object FileDataFrameReadWriteFormat {

  def apply(config: Config): FileDataFrameReadWriteFormat = {
    val fileType = config.getString("type").toLowerCase
    fileType match {
      case s if s.endsWith("csv") => new CSVDataFrameReadWriteFormat(config)
      case s if s.endsWith("parquet") => new ParquetDataFrameReadWriteFormat(config)
      case _ =>
        throw new IllegalArgumentException(
          "File Does not Support File Type :" + fileType
        )
    }
  }

}