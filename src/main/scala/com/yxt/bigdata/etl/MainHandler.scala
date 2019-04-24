package com.yxt.bigdata.etl

import scala.util.Properties
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}


object MainHandler extends App {
  Properties.setProp("scala.time", "true")

  val spark = SparkSession.builder()
    .appName("Spark-ETL")
    //        .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val jsonFilePath = args(0)
  val cmdParams = if (args.length > 1) args(1) else ""
  val job = new JobParser(jsonFilePath, cmdParams)
  val writer = job.getWriter

  var totalDf: DataFrame = _
  for (reader <- job.getReaders) {
    println(s"start fetching table ${reader.tableName}")
    // 数据库查询
    var subDf = reader.getDataFrame(spark)
    val readerColumns = reader.getColumnsFromDataFrame(subDf)

    // 当writer的columns为'*'时，遵循reader的配置
    if ("*".equals(writer.columns.mkString(","))) {
      writer.columns = readerColumns
    } else {
      val writerColumns = writer.columns
      if (readerColumns.length != writerColumns.length) {
        throw new Exception("列配置信息有误，因为您配置的任务中，源头读取字段数：%d 与 目标表要写入的字段数：%d 不相等，请检查您的配置并作出修改。")
      }

      val columnsWithAlias = new Array[String](readerColumns.length)
      for (i <- readerColumns.indices) {
        val rc = readerColumns(i)
        val wc = writerColumns(i)
        if (rc.equals(wc)) columnsWithAlias(i) = s"$rc"
        else columnsWithAlias(i) = s"$rc AS $wc"
      }
      subDf = subDf.selectExpr(columnsWithAlias: _*)
    }

    totalDf = if (totalDf == null) subDf else totalDf.union(subDf)
  }

  writer.saveTable(totalDf, writer.writeMode)

  spark.close()
}