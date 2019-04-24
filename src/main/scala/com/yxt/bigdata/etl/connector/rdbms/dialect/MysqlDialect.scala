package com.yxt.bigdata.etl.connector.rdbms.dialect


object MysqlDialect extends BaseDialect {
  val initConnectionSql: String = "set names utf8mb4"

  def formatDateDay(columnName: String): String = {
    s"DATE_FORMAT($columnName,'%Y-%m-%d')"
  }

  def formatDateMonth(columnName: String): String = {
    s"DATE_FORMAT($columnName,'%Y-%m')"
  }

  def computeTableSpace(tableName: String): String = {
    val Array(schema, name) = tableName.split("\\.")
    s"""
       |SELECT
       |	data_length / 1024 / 1024 AS MB
       |FROM
       |	information_schema.TABLES
       |WHERE
       |	LOWER(TABLE_SCHEMA) = '$schema'
       |AND LOWER(table_name) = '$name'
     """.stripMargin
  }
}
