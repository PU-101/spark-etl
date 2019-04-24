package com.yxt.bigdata.etl.connector.rdbms.reader

import java.util.Calendar
import com.typesafe.config.Config
import com.yxt.bigdata.etl.connector.base.AdvancedConfig
import com.yxt.bigdata.etl.connector.rdbms.dialect.BaseDialect

class PredicatesParser(val dialect: BaseDialect) {
  /*
  按实际情况选择相应的方案，避免出现数据倾斜
   */
  def parseCustomPredicates(predicatesConf: Config): Array[String] = {
    /*
    type: custom

    "predicates": {
      "type": "custom",
      "rules": [
        ...
      ]
    }
     */
    AdvancedConfig.getStringArray(predicatesConf, Key.PREDICATES_RULES)
  }

  def parseLongFieldPredicates(predicatesConf: Config): (String, Long, Long, Int) = {
    /*
    type: long

    "predicates": {
      "type": "long",
      "rules": {
        "columnName": "...",
        "lowerBound": "...",
        "upperBound": "...",
        "numPartitions": "..."
      }
    }
     */
    val columnName = AdvancedConfig.getString(predicatesConf, Key.PREDICATES_RULES + "." + "columnName")
    val lowerBound = AdvancedConfig.getString(predicatesConf, Key.PREDICATES_RULES + "." + "lowerBound").toLong
    val upperBound = AdvancedConfig.getString(predicatesConf, Key.PREDICATES_RULES + "." + "upperBound").toLong
    val numPartition = AdvancedConfig.getInt(predicatesConf, Key.PREDICATES_RULES + "." + "numPartitions")
    (columnName, lowerBound, upperBound, numPartition)
  }

  def parseUUIDPredicates(predicatesConf: Config): Array[String] = {
    /*
    type: uuid

    "predicates": {
      "type": "uuid",
      "rules": {
        "columnName": "...",
        "digit": ...
      }
    }
     */
    val columnName = AdvancedConfig.getString(predicatesConf, Key.PREDICATES_RULES + "." + "columnName")
    val digit = AdvancedConfig.getIntWithDefaultValue(predicatesConf, Key.PREDICATES_RULES + "." + "digit", 2)

    // uuid是由16进制数构成，0~9，a~f
    val elementChar = Array("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f")
    var combinations = elementChar
    for (_: Int <- 0 until digit - 1) {
      combinations = for {
        x <- combinations
        y <- elementChar
      } yield x + y
    }

    var predicates = Array[String](s"$columnName>='${combinations(combinations.length - 1)}'")
    for (i <- 0 until combinations.length - 1) {
      val start = combinations(i)
      val end = combinations(i + 1)
      predicates :+= s"$columnName>='$start' AND $columnName<'$end'"
    }

    predicates
  }

  def parseDatePredicates(predicatesConf: Config): Array[String] = {
    /*
      type: date
      定位到月，按月同步

      "predicates": {
        "type": "date",
        "rules": {
          "startYear": ...,
          "startMonth": ...,
          "startDay": ...,
          "endYear": ...,
          "endMonth": ...,
          "endDay": ...,
          "columnName": "...",
          "unit": "..."
        }
      }
       */
    val columnName = AdvancedConfig.getString(predicatesConf, Key.PREDICATES_RULES + "." + "columnName")
    val unit = AdvancedConfig.getString(predicatesConf, Key.PREDICATES_RULES + "." + "unit").toLowerCase
    val startYear = AdvancedConfig.getInt(predicatesConf, Key.PREDICATES_RULES + "." + "startYear")
    val startMonth = AdvancedConfig.getInt(predicatesConf, Key.PREDICATES_RULES + "." + "startMonth")
    val startDay = AdvancedConfig.getInt(predicatesConf, Key.PREDICATES_RULES + "." + "startDay")
    val endYear = AdvancedConfig.getInt(predicatesConf, Key.PREDICATES_RULES + "." + "endYear")
    val endMonth = AdvancedConfig.getInt(predicatesConf, Key.PREDICATES_RULES + "." + "endMonth")
    val endDay = AdvancedConfig.getInt(predicatesConf, Key.PREDICATES_RULES + "." + "endDay")

    val cal = Calendar.getInstance()
    cal.set(startYear, startMonth - 1, startDay)
    val endCal = Calendar.getInstance()
    endCal.set(endYear, endMonth - 1, endDay)

    var predicates = Array[String]()
    while (!cal.after(endCal)) {
      val year = cal.get(Calendar.YEAR)
      val month = cal.get(Calendar.MONTH) + 1
      val day = cal.get(Calendar.DATE)

      unit match {
        case "month" =>
          predicates :+= dialect.formatDateMonth(columnName) + f"='$year-$month%02d'"
          cal.add(Calendar.MONTH, 1)
        case "day" =>
          predicates :+= dialect.formatDateDay(columnName) + f"='$year-$month%02d-$day%02d'"
          cal.add(Calendar.DATE, 1)
      }
    }

    predicates
  }

  def parsePaginationPredicates(predicatesConf: Config): Array[String] = {
    val interval = AdvancedConfig.getString(predicatesConf, Key.PREDICATES_RULES + "." + "interval")
    Array(interval)
  }
}
