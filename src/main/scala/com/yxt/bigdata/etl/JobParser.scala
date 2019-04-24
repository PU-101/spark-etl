package com.yxt.bigdata.etl

import java.io.{BufferedReader, FileReader}

import scala.collection.JavaConversions._
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValueFactory
import com.yxt.bigdata.etl.connector.base.component.{ETLReader, ETLWriter}
import com.yxt.bigdata.etl.connector.hbase.writer.HbaseWriter
import com.yxt.bigdata.etl.connector.hive.reader.HiveReader
import com.yxt.bigdata.etl.connector.hive.writer.HiveWriter
import com.yxt.bigdata.etl.connector.rdbms.reader.RdbReader
import com.yxt.bigdata.etl.connector.rdbms.writer.RdbWriter

import scala.util.matching.Regex


class JobParser(private val jobConfPath: String, private val cmd: String = "") {
  private val configFileContent = JobParser.replaceVariables(JobParser.readFileContent(jobConfPath), JobParser.parseCMDParams(cmd))
  private val jobConf = ConfigFactory.parseString(configFileContent)
  println(jobConf.toString)


  def getReaders: List[ETLReader] = {
    /*
    "reader": [
      {
        "name": "xxx",
        "tableNames": ["A.a", "B.a"],
        ...
      }
    ]
     */

    var newReaderConfs = List[Config]()
    for (readerConf <- jobConf.getConfigList("reader").toList) {
      if (readerConf.hasPath("tableNames")) {
        for (tableName <- readerConf.getStringList("tableNames").toList) {
          newReaderConfs = readerConf.withValue("tableName", ConfigValueFactory.fromAnyRef(tableName)) :: newReaderConfs
        }
      } else {
        newReaderConfs = readerConf :: newReaderConfs
      }
    }

    val readers = for (readerConf <- newReaderConfs) yield {
      val name = readerConf.getString("name").toLowerCase
      name match {
        case "mysqlreader" | "oraclereader" =>
          new RdbReader(readerConf)
        case "hivereader" => new HiveReader(readerConf)
        case _ => throw new Exception("您在 reader 中配置的 'name' 错误，请检查您的配置文件并作出修改。")
      }
    }

    readers
  }

  def getWriter: ETLWriter = {
    val writerConf = jobConf.getConfig("writer")
    val name = writerConf.getString("name").toLowerCase
    name match {
      case "mysqlwriter" | "oraclewriter" => new RdbWriter(writerConf)
      case "hivewriter" => new HiveWriter(writerConf)
      case "hbasewriter" => new HbaseWriter(writerConf)
      case _ => throw new Exception("您在 writer 中配置的 'name' 错误，请检查您的配置文件并作出修改。")
    }
  }
}


object JobParser {
  def readFileContent(path: String): String = {
    val reader = new BufferedReader(new FileReader(path))
    val sb = new StringBuilder()
    var tempLine: String = reader.readLine()
    while (tempLine != null) {
      sb.append(tempLine)
      tempLine = reader.readLine()
    }

    sb.toString()
  }

  def parseCMDParams(cmd: String): Map[String, String] = {
    // cmd
    // "-Dxxx=\"xxx\""
    val pattern = "-D(.*?)=\"(.*?)\"".r
    val matcher = pattern.findAllMatchIn(cmd)
    var params = Map[String, String]()
    for (m <- matcher) {
      val key = m.group(1)
      val value = m.group(2)
      params += (key -> value)
    }

    println(params)
    params
  }

  def replaceVariables(fileContent: String, cmdParams: Map[String, String]): String = {
    var content = fileContent
    for ((key, value) <- cmdParams) {
      val matcherPattern = new Regex("\\$KEY".replace("KEY", key))
      content = matcherPattern.replaceAllIn(content, value)
    }

    content
  }
}
