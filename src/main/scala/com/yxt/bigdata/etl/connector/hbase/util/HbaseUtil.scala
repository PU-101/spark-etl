package com.yxt.bigdata.etl.connector.hbase.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HBaseAdmin}

class HbaseUtil(zookeeperHost: String, zookeeperPort: String) {
  private val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum", zookeeperHost)
  conf.set("hbase.zookeeper.property.clientPort", zookeeperPort)
  lazy private val connection = ConnectionFactory.createConnection(conf)

  def getConnection: Connection = connection

  def close(): Unit = connection.close()

  def getConfiguration: Configuration = conf

  def truncateTable(tableName: String, preserveSplits: Boolean = true): Unit = {
    val hbaseAdmin = getConnection.getAdmin
    val tName = TableName.valueOf(tableName)
    if (hbaseAdmin.isTableEnabled(tName)) {
      hbaseAdmin.disableTable(tName)
    }
    // truncate之后自动enable
    hbaseAdmin.truncateTable(tName, preserveSplits)
  }
}
