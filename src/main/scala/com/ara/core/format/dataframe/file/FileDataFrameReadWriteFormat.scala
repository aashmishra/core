package com.ara.core.format.dataframe.file

import com.typesafe.config.Config
import com.ara.core.format.dataframe.DataFrameReadWriteFormat

trait FileDataFrameReadWriteFormat extends DataFrameReadWriteFormat{

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