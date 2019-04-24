package com.yxt.bigdata.etl.connector.base.db

import java.sql.{Connection, DatabaseMetaData, DriverManager, ResultSet, ResultSetMetaData, SQLException, Statement}
import java.util.Properties

import com.typesafe.config.Config
import org.apache.spark.sql.types.StructType

import com.yxt.bigdata.etl.connector.base.AdvancedConfig
import com.yxt.bigdata.etl.connector.rdbms.dialect._


class DBUtil(private val conf: Config) extends Serializable {
  val dbType: String = AdvancedConfig.getString(conf, Key.NAME).trim.replaceAll("reader|writer", "")

  val dialect: BaseDialect = dbType match {
    case "oracle" => OracleDialect
    case "mysql" => MysqlDialect
  }

  val userName: String = AdvancedConfig.getString(conf, Key.USERNAME)

  val password: String = AdvancedConfig.getString(conf, Key.PASSWORD)

  val driver: String = AdvancedConfig.getString(conf, Key.DRIVER)

  val jdbcUrl: String = appendJDBCSuffix(AdvancedConfig.getString(conf, Key.JDBC_URL))

  val jdbcProperties: Properties = {
    // 构建连接属性
    val connectionProperties = new Properties()
    connectionProperties.setProperty("user", userName)
    connectionProperties.setProperty("password", password)
    connectionProperties.setProperty("driver", driver)
    connectionProperties
  }

  def getConnection(): Connection = {
    try {
      Class.forName(driver)
    } catch {
      case _: ClassNotFoundException => throw new ClassNotFoundException(s"找不到驱动程序类 $driver，加载驱动失败。")
    }

    val conn = DriverManager.getConnection(jdbcUrl, jdbcProperties)
    if (dialect.initConnectionSql != null) {
      val stmt = conn.createStatement()
      try {
        stmt.execute(dialect.initConnectionSql)
      } finally {
        stmt.close()
      }
    }
    conn
  }

  def closeDB(rs: ResultSet, stmt: Statement, conn: Connection): Unit = {
    if (null != rs) {
      try {
        rs.close()
      } catch {
        case e: SQLException => e.printStackTrace()
      }
    }

    if (null != stmt) {
      try {
        stmt.close()
      } catch {
        case e: SQLException => e.printStackTrace()
      }
    }

    if (null != conn) {
      try {
        conn.close()
      } catch {
        case e: SQLException => e.printStackTrace()
      }
    }
  }

  def executeSql(sql: String): Unit = {
    val conn: Connection = getConnection()
    var stmt: Statement = null

    try {
      stmt = conn.createStatement()
      stmt.execute(sql)
    } finally {
      closeDB(null, null, conn)
    }
  }

  def getCount(tableName: String, where: String = ""): Int = {
    val conn: Connection = getConnection()
    var stmt: Statement = null
    var rs: ResultSet = null
    var count: Int = 0

    try {
      stmt = conn.createStatement()
      val sql = if (where.length == 0) s"SELECT COUNT(1) as c FROM $tableName" else s"SELECT COUNT(1) as c FROM $tableName WHERE $where"
      rs = stmt.executeQuery(sql)
      count = if (rs.next) rs.getInt("c") else 0
    } finally {
      closeDB(null, null, conn)
    }

    count
  }

  def getAllColumns(tableName: String): Array[String] = {
    val conn: Connection = getConnection()
    var stmt: Statement = null
    var rs: ResultSet = null
    var columns = Array[String]()

    try {
      stmt = conn.createStatement()
      rs = stmt.executeQuery(s"SELECT * FROM $tableName WHERE 1=0")
      val rsmd = rs.getMetaData
      for (i <- 0 until rsmd.getColumnCount) {
        columns :+= rsmd.getColumnName(i + 1).toLowerCase
      }
    } finally {
      closeDB(null, null, conn)
    }

    columns
  }

  def getTableSize(tableName: String): Int = {
    val conn: Connection = getConnection()
    var stmt: Statement = null
    var rs: ResultSet = null
    var space: Int = 0

    try {
      stmt = conn.createStatement()
      rs = stmt.executeQuery(dialect.computeTableSpace(tableName))
      if (rs.next) space = rs.getInt("MB")
      else throw new Exception("无法获取到表空间大小！")
    } finally {
      closeDB(rs, stmt, conn)
    }

    space
  }

  def getDDL(tableName: String): String = {
    val conn: Connection = getConnection()
    var stmt: Statement = null
    var rs: ResultSet = null
    val ddl = new StringBuilder()

    try {
      stmt = conn.createStatement()
      rs = stmt.executeQuery(s"select * from $tableName where 1=0")

      val meta: DatabaseMetaData = conn.getMetaData
      val rsmd: ResultSetMetaData = rs.getMetaData
      val ncols = rsmd.getColumnCount

      // columns
      ddl.append(s"CREATE TABLE `%s` (\n")
      for (i <- 0 until ncols) {
        val columnName = rsmd.getColumnLabel(i + 1)
        val typeName = rsmd.getColumnTypeName(i + 1)
        val fieldSize = {
          if ("DATETIME".equals(typeName)) 6
          else rsmd.getPrecision(i + 1)
        }
        val nullable = if (rsmd.isNullable(i + 1) == 0) "NOT NULL" else ""
        val isAutoIncrement = if (rsmd.isAutoIncrement(i + 1)) "AUTO_INCREMENT" else ""

        ddl.append(s"`$columnName` $typeName($fieldSize) $nullable $isAutoIncrement,\n")
      }

      // primaryKeys
      val primaryKeys = meta.getPrimaryKeys("", "", tableName)
      while (primaryKeys.next()) {
        val pkName = primaryKeys.getString("COLUMN_NAME")
        ddl.append(s"PRIMARY KEY (`$pkName`),\n")
      }
      // 删除最后一个逗号
      ddl.deleteCharAt(ddl.length - 2)
      ddl.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8;")
    } finally {
      closeDB(rs, stmt, conn)
    }

    ddl.toString()
  }

  def getDDL(rddSchema: StructType): String = {
    for (structField <- rddSchema.fields) {
      structField.dataType
    }
    ""
  }

  def createTableIfNotExists(tableName: String, ddl: String): Unit = {
    val conn: Connection = getConnection()
    conn.setAutoCommit(false)
    val stmt: Statement = conn.createStatement()

    var flag = true
    val checkSql = s"select * from $tableName where 1=0"
    try {
      stmt.execute(checkSql)
      flag = false
    } catch {
      case _: Exception =>
        try {
          stmt.execute(ddl.format(tableName))
          conn.commit()
        } catch {
          case e: Exception =>
            e.printStackTrace()
            conn.rollback()
        }
    } finally {
      closeDB(null, stmt, conn)
    }
  }

  def appendJDBCSuffix(jdbc: String): String = {
    val dataBaseType = jdbc.split(":")(1)
    var result: String = jdbc
    var suffix: String = ""

    dataBaseType match {
      case "mysql" =>
        /*
        rewriteBatchedStatements=true
        mysql在默认情况下，executeBatch还是每次和服务器通信
         */
        suffix = "autoReconnect=true&useSSL=false&yearIsDateType=false&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&rewriteBatchedStatements=true"
        if (jdbc.contains("?")) result = jdbc + "&" + suffix
        else result = jdbc + "?" + suffix
      case _ =>
    }

    result
  }
}
