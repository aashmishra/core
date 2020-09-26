package com.ara.core.unittest.sharedcontext

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.scalatest._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}


trait SharedSparkSession extends FunSuite with BeforeAndAfterAll { this: Suite =>


  @transient private var _ss: SparkSession = _
  @transient private var _sqlContext: SQLContext = _

  /**
   * Get Spark Session before start
   */

  override def beforeAll(): Unit = {
    super.beforeAll()
  _ss = SparkSession
    .builder()
    .appName("test")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()
    _sqlContext = _ss.sqlContext
    Logger.getLogger("org").setLevel(Level.ERROR)
  }

  /**
   * close spark Session
   */
  override def afterAll(): Unit = {
   if(_ss != null) {
   _ss.stop()
     _ss = null
   }
    super.afterAll()
   }


  /**
   * Build with default values and set master as local
   *
   * Any Suite which wants to add extra configuration should override this is before all style
   * example :- override def sparkConf = super.sparkConf.set("spark.master", "local[1]")
   *
   * @return SparkSession Object
   */
    def sparkSession: SparkSession = _ss

  /**
   * @return SQLContext
   */
  def sqlContext: SQLContext = _sqlContext

  /**
   * @return SparkContext
   */

  def sc: SparkContext = _ss.sparkContext

  /**
   * Given relative path for a files in resources folder, resolves into Absolute path
   * @param fileName filename
   * @return absolute path
   */

  def resolvePath(fileName:String):String={
    val resourcePath = getClass.getResource(fileName)
    new File(resourcePath.toURI).getAbsolutePath
  }

}

