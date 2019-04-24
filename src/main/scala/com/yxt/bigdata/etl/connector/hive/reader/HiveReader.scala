package com.yxt.bigdata.etl.connector.hive.reader

import com.typesafe.config.Config
import com.yxt.bigdata.etl.connector.base.AdvancedConfig
import com.yxt.bigdata.etl.connector.base.component.ETLReader
import org.apache.spark.sql.{DataFrame, SparkSession}


class HiveReader(conf: Config) extends ETLReader {
  override val tableName: Option[String] = AdvancedConfig.getString(conf, Key.TABLE_NAME, isNecessary = false)

  override var columns: Option[Array[String]] = {
    val cols = AdvancedConfig.getString(conf, Key.COLUMNS, isNecessary = false)
    cols.map(_.split(",").map(_.trim.toLowerCase))
  }

  override var where: Option[String] = AdvancedConfig.getString(conf, Key.WHERE, isNecessary = false)

  override val querySql: Option[String] = AdvancedConfig.getString(conf, Key.QUERY_SQL, isNecessary = false)

  override def getDataFrame(spark: SparkSession): DataFrame = {
    /*
    query sql
    指定querySql时，tableName、columns和where则忽略;
    若querySql为空，则tableName和columns不能为空。
     */
    val df: DataFrame = querySql match {
      case Some(_) => getDataFrameWithQuerySql(spark)
      case None =>
        tableName match {
          case Some(_) => getDataFrameWithTableName(spark)
          case None => throw new Exception("您未配置 tableName ；请检查您的配置文件并作出修改，建议 tableName 的格式为 db.table 。")
        }
    }

    // 列名最小化
    val loweredColumns = df.columns.map(col => col.toLowerCase)
    df.toDF(loweredColumns: _*)
    // 输出schema
    df.printSchema()

    df
  }

  def getDataFrameWithTableName(spark: SparkSession): DataFrame = {
    var df: DataFrame = null

    // columns
    columns match {
      case Some(cols) => df = spark.sql(s"select ${cols.mkString(",")} from ${tableName.get}")
      case None => throw new Exception("您未配置 columns ；请检查你的配置文件并作出修改，注意 columns 以英文逗号作为分隔符。")
    }

    // where
    where.foreach(condition => df = df.filter(condition))

    df
  }

  def getDataFrameWithQuerySql(spark: SparkSession): DataFrame = {
    spark.sql(querySql.get)
  }
}
