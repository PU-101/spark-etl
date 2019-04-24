package com.yxt.bigdata.etl.connector.base.db

object DataBaseType extends Enumeration {
  type DatabaseType = Value
  val MYSQL, ORACLE, HIVE = Value
}
