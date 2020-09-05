package com.ara.core.unittest

import java.io.{File, IOException}

import org.apache.commons.io.FileUtils

object FileOperations {


  def removeParentFolder(path: String): Unit = {
    val file = new File(path).getParentFile
    try{
      FileUtils.deleteDirectory(file)
    } catch {
      case ioException: IOException => println("Unable to delete some Files " + path)
    }
  }
}
