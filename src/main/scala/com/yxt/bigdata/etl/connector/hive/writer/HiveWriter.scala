package com.yxt.bigdata.etl.connector.hive.writer

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.StructType
import com.yxt.bigdata.etl.connector.base.AdvancedConfig
import com.yxt.bigdata.etl.connector.base.component.ETLWriter


class HiveWriter(conf: Config) extends ETLWriter {
  val tableName: String = AdvancedConfig.getString(conf, Key.TABLE_NAME)

  var columns: Array[String] = {
    AdvancedConfig.getString(conf, Key.COLUMNS).split(",").map(_.trim.toLowerCase)
  }

  val writeMode: String = AdvancedConfig.getStringWithDefaultValue(conf, Key.WRITE_MODE, Constant.WRITE_MODE)

  val tempSuffix: String = AdvancedConfig.getStringWithDefaultValue(conf, Key.TEMP_SUFFIX, Constant.TEMP_SUFFIX)

  def saveTable(dataFrame: DataFrame, mode: String): Unit = {
    val spark = dataFrame.sparkSession
    val originalTableName = tableName

    // 打印行数
    //    println(s"Hive $tableName 的总行数为：" + spark.sql(s"select count(1) from $tableName").collect().mkString(","))

    /*
    存储格式：textfile、orc
    textfile: dropDelims(dataFrame)
    orc: 不需要指定考虑分隔符
    */
    val dataFrameWithoutDelims = dropDelims(dataFrame)

    // 使用SQL，灵活度高；
    val tmpTableName = genTmpTableName(tableName)
    dataFrameWithoutDelims.createOrReplaceTempView(tmpTableName)

    // 写入模式
    //    spark.sqlContext.setConf("hive.enforce.bucketing", "true")
    mode match {
      case "overwrite" =>
        // 对于支持事务的表，无法使用insert overwrite
        spark.sql(
          s"""
             |INSERT OVERWRITE TABLE $originalTableName
             |SELECT * FROM $tmpTableName
           """.stripMargin)
      case "append" =>
        spark.sql(
          s"""
             |INSERT INTO TABLE $originalTableName
             |SELECT * FROM $tmpTableName
           """.stripMargin)
      case "temp" =>
        dataFrameWithoutDelims.write.format("orc").mode(SaveMode.Overwrite).saveAsTable(s"${originalTableName}_$tempSuffix")
      case "file" =>
        dataFrameWithoutDelims.write
          .options(Map(
            "delimiter" -> "\t",
            "nullValue" -> "\\N",
            "maxCharsPerColumn" -> "10000"))
          .csv(s"/user/root/tidb/$originalTableName")
      case _ => throw new Exception(s"写入模式有误，您配置的写入模式为：$mode，而目前hiveWriter支持的写入模式仅为：overwrite 、 append 和 temp，请检查您的配置项并作出相应的修改。")
    }

    // 打印行数
    //    println(s"Hive $tableName 的总行数为：" + spark.sql(s"select count(1) from $tableName").collect().mkString(","))

    // 分区
    //    partitionBy match {
    //      case Some(partitionCols) =>
    //        val sortedCols = columns.filter(!partitionCols.contains(_)) ++ partitionCols
    //        spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    //        spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    //        spark.sql(
    //          s"""
    //             |INSERT INTO TABLE $originalTableName PARTITION(${partitionCols.mkString(",")})
    //             |SELECT ${sortedCols.mkString(",")} FROM $tmpTableName
    //           """.stripMargin)
    //      case None =>
    //        spark.sql(
    //          s"""
    //             |INSERT OVERWRITE TABLE $originalTableName
    //             |SELECT * FROM $tmpTableName
    //           """.stripMargin)
    //    }
  }

  def genTmpTableName(originalTableName: String): String = {
    /*
    It is not allowed to add database prefix for the TEMPORARY view name
     */
    var tmpTableName: String = null
    val sepTableName = originalTableName.split("[.]")
    val len = sepTableName.length

    if (len == 1) tmpTableName = s"${originalTableName}_tmp"
    else if (len == 2) {
      val Array(_, table) = sepTableName
      tmpTableName = s"${table}_tmp"
    }
    else throw new Exception(s"表名配置信息有误，您的表名为：$originalTableName , 而目前支持的表名格式有两种：'database.table' 和 'table'，请检查您的配置并作出修改。")

    tmpTableName
  }

  def dropDelims(dataFrame: DataFrame): DataFrame = {
    val schema = dataFrame.schema
    val fields = schema.fields
    val exprs = new Array[String](fields.length)
    for (i <- fields.indices) {
      val field = fields(i)
      if ("string".equals(field.dataType.typeName)) {
        exprs(i) = s"regexp_replace(${field.name}, '\\n|\\r|\\t|\\x00', '\\0') AS ${field.name}"
      } else exprs(i) = s"${field.name}"
    }
    dataFrame.selectExpr(exprs: _*)
  }
}
