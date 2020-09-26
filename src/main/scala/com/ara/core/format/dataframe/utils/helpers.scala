package com.ara.core.format.dataframe.utils

import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters._

object helpers {

  def checkAndGetConfList(config: Config, ConfigName:String):List[Config] ={
    if (config.hasPath(ConfigName)) {
      config.getConfigList(ConfigName).asScala.toList
    } else {
      List.empty[Config]
    }
  }

  def checkAndGetConf(config: Config, ConfigName:String):Config =
    if (config.hasPath(ConfigName)) config.getConfig(ConfigName)  else ConfigFactory.empty()

}
