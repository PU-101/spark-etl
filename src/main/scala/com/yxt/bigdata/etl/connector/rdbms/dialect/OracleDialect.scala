package com.yxt.bigdata.etl.connector.rdbms.dialect

object OracleDialect extends BaseDialect {
  val initConnectionSql: String = null

  def formatDateDay(columnName: String): String = {
    s"to_char($columnName, 'yyyy-MM-dd')"
  }

  def formatDateMonth(columnName: String): String = {
    s"to_char($columnName, 'yyyy-MM')"
  }

  def computeTableSpace(tableName: String): String = {
    s"""
       |SELECT
       |	num_rows * avg_row_len / 1024 / 1024 AS MB
       |FROM
       |	user_tables
       |WHERE
       |	lower(table_name) = '${tableName.split(".")(1)}'
     """.stripMargin
  }
}
