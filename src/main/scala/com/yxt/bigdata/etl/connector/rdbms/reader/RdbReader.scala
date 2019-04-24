package com.yxt.bigdata.etl.connector.rdbms.reader

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

import com.yxt.bigdata.etl.connector.base.component.ETLReader
import com.yxt.bigdata.etl.connector.base.db.DBUtil
import com.yxt.bigdata.etl.connector.base.AdvancedConfig


class RdbReader(conf: Config) extends DBUtil(conf) with ETLReader {
  val tableName: Option[String] = AdvancedConfig.getString(conf, Key.TABLE_NAME, isNecessary = false)

  var columns: Option[Array[String]] = {
    val cols = AdvancedConfig.getString(conf, Key.COLUMNS, isNecessary = false)
    cols.map(_.split(",").map(_.trim.toLowerCase))
  }

  var where: Option[String] = AdvancedConfig.getString(conf, Key.WHERE, isNecessary = false)

  val querySql: Option[String] = AdvancedConfig.getString(conf, Key.QUERY_SQL, isNecessary = false)

  val predicatesConf: Option[Config] = AdvancedConfig.getConfig(conf, Key.PREDICATES, isNecessary = false)

  val fetchSize: String = AdvancedConfig.getStringWithDefaultValue(conf, Key.FETCH_SIZE, Constant.FETCH_SIZE)

  def getDataFrame(spark: SparkSession): DataFrame = {
//    tableName match {
//      case Some(tName) =>
//        println(s"关系型数据库 $tName 的总行数： " + getCount(tName).toString)
//        where match {
//          case Some(condition) => println(s"$tName 的条件行数： " + getCount(tName, condition).toString)
//          case None =>
//        }
//      case None =>
//    }

    // 打印行数
    tableName.foreach(tName => {
      println(s"关系型数据库 $tName 的总行数： " + getCount(tName).toString)
      where.foreach(condition => println(s"$tName 的条件行数： " + getCount(tName, condition).toString))
    })


    /*
    query sql
    指定querySql时，tableName、columns和where则忽略;
    若querySql为空，则tableName和columns不能为空。
     */
    val table = querySql match {
      case Some(sql) =>
        // 配置了querySql
        // 使用查询语句必须要用括号，否则会被识别为table;
        // querySql不能包含分号，会被识别为无效字符.
        "(" + sql.replace(";", "") + ") spark_etl_tmp"
      case None =>
        // 未配置querySql
        val tbName = tableName match {
          case Some(value) => value
          case None => throw new Exception("您未配置 tableName ；请检查您的配置文件并作出修改，建议 tableName 的格式为 db.table 。")
        }

        val exceptionOfColumns = new Exception("您未配置 columns ；请检查你的配置文件并作出修改，注意 columns 以英文逗号作为分隔符。")
        val cols = columns match {
          case Some(value) =>
            if (value.length == 0) {
              throw exceptionOfColumns
            } else {
              value.mkString(",") // 对于'*'也适用
            }
          case None => throw exceptionOfColumns
        }

        val condition = where match {
          case Some(value) => s"where $value"
          case None => ""
        }

        s"(SELECT $cols FROM $tbName $condition) spark_etl_tmp"
    }

    var df: DataFrame = predicatesConf match {
      case Some(predConf) =>
        val typeName = AdvancedConfig.getString(predConf, Key.PREDICATES_TYPE)
        val predicatesParser = new PredicatesParser(dialect)
        typeName match {
          case "long" =>
            /*
            多并发
            按数字字段切分
             */
            val (columnName, lowerBound, upperBound, numPartitions) = predicatesParser.parseLongFieldPredicates(predConf)
            spark.read.option("fetchsize", fetchSize).jdbc(
              jdbcUrl,
              table,
              columnName,
              lowerBound,
              upperBound,
              numPartitions,
              jdbcProperties
            )
          case "uuid" =>
            /*
            多并发
            按UUID字段首字母切分
             */
            val predicates = predicatesParser.parseUUIDPredicates(predConf)
            spark.read.option("fetchsize", fetchSize).jdbc(
              jdbcUrl,
              table,
              predicates,
              jdbcProperties
            )
          case "date" =>
            /*
            多并发
            按月切分
             */
            val predicates = predicatesParser.parseDatePredicates(predConf)
            spark.read.option("fetchsize", fetchSize).jdbc(
              jdbcUrl,
              table,
              predicates,
              jdbcProperties
            )
          case "custom" =>
            /*
            多并发
            自定义切分规则
             */
            val predicates = predicatesParser.parseCustomPredicates(predConf)
            spark.read.option("fetchsize", fetchSize).jdbc(
              jdbcUrl,
              table,
              predicates,
              jdbcProperties
            )
          case "pagination" =>
            // 高效分页sql
            def paginationQuery(index: Int, interval: Int): String = {
              s"(SELECT * FROM ( SELECT spark_etl_tmp.*, ROWNUM AS SPARK_ETL_RN FROM $table WHERE ROWNUM < ${index * interval}) a WHERE SPARK_ETL_RN >= ${(index - 1) * interval})"
            }

            // 合并每个分页结果
            def unionTableReducer: (DataFrame, DataFrame) => DataFrame = (x: DataFrame, y: DataFrame) => x.union(y)

            val predicates = predicatesParser.parsePaginationPredicates(predConf)
            val interval = predicates(0).toInt
            val count = getCount(table)
            val df = (1 to count / interval + 1).map(index => {
              spark.read.option("fetchsize", fetchSize).jdbc(
                jdbcUrl,
                paginationQuery(index, interval),
                jdbcProperties
              )
            }).reduce(unionTableReducer)

            // 去除分页里的rownum列
            df.selectExpr(df.columns.filter(_ != "SPARK_ETL_RN"): _*)
          case _ => throw new Exception(s"您配置的 predicates.type $typeName 错误，目前仅支持 'custom'、'long'、'uuid'，请检查你的配置并作出修改。")
        }
      case None =>
        /*
        无并发
        未配置predicates默认单任务
         */
        spark.read.option("fetchsize", fetchSize).jdbc(
          jdbcUrl,
          table,
          jdbcProperties
        )
    }

    // 列名最小化，为了和writer里的columns一致
    df.toDF(df.columns.map(col => col.toLowerCase): _*)
    // 输出schema
    df.printSchema()

    df
  }
}
