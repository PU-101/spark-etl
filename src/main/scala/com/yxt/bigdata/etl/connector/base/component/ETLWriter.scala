package com.yxt.bigdata.etl.connector.base.component

import org.apache.spark.sql.DataFrame

trait ETLWriter {
  val tableName: String

  /*
  该字段可以为'*'
  当为'*'时，需要用reader获取到的dataframe的columns来覆盖
   */
  var columns: Array[String]

  val writeMode: String

  def saveTable(dataFrame: DataFrame, mode: String): Unit
}
