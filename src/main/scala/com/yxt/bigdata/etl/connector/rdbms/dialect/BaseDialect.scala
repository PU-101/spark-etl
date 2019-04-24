package com.yxt.bigdata.etl.connector.rdbms.dialect

trait BaseDialect extends Serializable {
  val initConnectionSql: String

  def formatDateDay(columnName: String): String

  def formatDateMonth(columnName: String): String

  def computeTableSpace(tableName: String): String
}
