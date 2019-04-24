package com.yxt.bigdata.etl.connector.base.component

import org.apache.spark.sql.{DataFrame, SparkSession}

trait ETLReader {
  /*
  querySql的优先级最高
  若querySql不为空，则忽视columns和where
   */
  val tableName: Option[String]

  var columns: Option[Array[String]]

  var where: Option[String]

  val querySql: Option[String]

  def getDataFrame(spark: SparkSession): DataFrame

  /*
  reader可能指定columns，也可能指定querySql
  因此，需要从dataFrame中获取具体的字段
   */
  def getColumnsFromDataFrame(dataFrame: DataFrame): Array[String] = dataFrame.columns
}
