package com.yxt.bigdata.etl.connector.base

import collection.JavaConversions._

import com.typesafe.config.Config

object AdvancedConfig {
  private val errorMessage = "配置中缺失 %s ，该配置项是是必须的，请检查您的配置并作出修改。"

  /*
  string
   */
  def getString(conf: Config, key: String, isNecessary: Boolean): Option[String] = {
    try {
      Option(conf.getString(key))
    } catch {
      case _: Exception =>
        if (isNecessary) {
          throw new Exception(errorMessage.format(key))
        } else Option(null)
    }
  }

  def getString(conf: Config, key: String): String = {
    getString(conf, key, isNecessary = true).get
  }

  def getStringWithDefaultValue(conf: Config, key: String, defaultValue: String): String = {
    val value: Option[String] = getString(conf, key, isNecessary = false)
    value.getOrElse(defaultValue)
  }

  /*
  stringList
   */
  def getStringArray(conf: Config, key: String, isNecessary: Boolean): Option[Array[String]] = {
    try {
      Option(conf.getStringList(key).toList.toArray)
    } catch {
      case _: Exception =>
        if (isNecessary) {
          throw new Exception(errorMessage.format(key))
        } else Option(null)
    }
  }

  def getStringArray(conf: Config, key: String): Array[String] = {
    getStringArray(conf, key, isNecessary = true).get
  }

  /*
  int
   */
  def getInt(conf: Config, key: String, isNecessary: Boolean): Option[Int] = {
    getString(conf, key, isNecessary).map(_.toInt)
  }

  def getInt(conf: Config, key: String): Int = {
    getInt(conf, key, isNecessary = true).get
  }

  def getIntWithDefaultValue(conf: Config, key: String, defaultValue: Int): Int = {
    val value: Option[Int] = getInt(conf, key, isNecessary = false)
    value.getOrElse(defaultValue)
  }

  /*
  config
   */
  def getConfig(conf: Config, key: String, isNecessary: Boolean): Option[Config] = {
    try {
      Option(conf.getConfig(key))
    } catch {
      case _: Exception =>
        if (isNecessary) {
          throw new Exception(errorMessage.format(key))
        } else Option(null)
    }
  }

  def getConfig(conf: Config, key: String): Config = {
    getConfig(conf, key, isNecessary = true).get
  }

  /*
  universal
   */
  def getProp(conf: Config, func: (String) => Any, key: String, isNecessary: Boolean): Option[Any] = {
    try {
      Option(func(key))
    } catch {
      case _: Exception =>
        if (isNecessary) {
          throw new Exception(errorMessage.format(key))
        } else Option(null)
    }
  }
}
