package com.yxt.bigdata.etl.connector.hbase.writer

import com.typesafe.config.Config
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.spark.sql.types.StructType
import org.apache.hadoop.fs.{FileSystem, Path}
import com.yxt.bigdata.etl.connector.base.AdvancedConfig
import com.yxt.bigdata.etl.connector.base.component.ETLWriter
import org.apache.spark.sql.DataFrame
import com.yxt.bigdata.etl.connector.hbase.util.HbaseUtil
import org.apache.hadoop.fs.permission.FsPermission


class HbaseWriter(conf: Config) extends ETLWriter with Serializable {

  implicit class Regex(sc: StringContext) {
    def r = new util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
  }

  val tableName: String = AdvancedConfig.getString(conf, Key.TABLE_NAME)

  var columns: Array[String] = AdvancedConfig.getString(conf, Key.COLUMNS).split(",").map(_.trim.toLowerCase)

  val writeMode: String = AdvancedConfig.getStringWithDefaultValue(conf, Key.WRITE_MODE, Constant.WRITE_MODE)

  val hfilesDir: String = "/tmp/hfiles"

  val zookeeperHost: String = AdvancedConfig.getString(conf, Key.ZOOKEEPER_HOST)

  val zookeeperPort: String = AdvancedConfig.getStringWithDefaultValue(conf, Key.ZOOKEEPER_PORT, Constant.ZOOKEEPER_PORT)

  private val hbaseUtil = new HbaseUtil(zookeeperHost, zookeeperPort)

  def saveTable(dataFrame: DataFrame, mode: String): Unit = {
    mode match {
      case "overwrite" =>
        // hive无法truncate表
        hbaseUtil.truncateTable(tableName)
        println(s"truncate table $tableName")
      case "replace" =>
      case _ => throw new Exception(s"写入模式有误，您配置的写入模式为：$mode，而目前hbaseWriter支持的写入模式仅为：overwrite 和 replace，请检查您的配置项并作出相应的修改。")
    }

    //  dataFrame.rdd
    //    .map(row => {
    //      // schema获取到的字段名是大写，hive里字段名是小写
    //      val schema = row.schema
    //      val thePut = new Put(row.getString(0).getBytes)
    //      for (i <- 1 until schema.size) {
    //        val value = row.get(i)
    //        thePut.addColumn("cf".getBytes, schema(i).name.toLowerCase.getBytes, if (value == null) "".getBytes else value.toString.getBytes)
    //      }
    //      thePut
    //    })
    //    .foreachPartition(thePutRecords => {
    //      val hbaseConnection = HbaseUtil.getConnection
    //      val hbaseTable = hbaseConnection.getTable(TableName.valueOf(tableName))

    //      hbaseTable.put(thePutRecords.toList)
    //      hbaseTable.close()
    //    })

    //     hbase bulk load
    val rdd = dataFrame.rdd
      .repartition(500)
      .flatMap(row => {
        val schema = row.schema.map(_.name)
        val rowKey = row.getAs[String]("ID").getBytes

        val results = for {
          i <- 1 until schema.size
          if schema(i) != "ID" && !row.isNullAt(i)
          key = schema(i)
          value = row.get(i).toString
          kv: KeyValue = new KeyValue(rowKey, "cf".getBytes, key.getBytes, value.getBytes)
        } yield (new ImmutableBytesWritable(rowKey), kv, key)

        // 直接对kv的key做排序，会有问题
        results.sortBy(_._3).map(x => (x._1, x._2))
      })
      .sortByKey()

    // 删除hfiles目录
    val hdfs: FileSystem = FileSystem.get(dataFrame.sparkSession.sparkContext.hadoopConfiguration)
    val hfilesPath = new Path(hfilesDir)
    if (hdfs.exists(hfilesPath)) hdfs.delete(hfilesPath, true)

    val conf = hbaseUtil.getConfiguration
    conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 1024)
    rdd.saveAsNewAPIHadoopFile(
      hfilesDir,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      conf
    )

    // 修改hfile目录的权限
    val rwx = new FsPermission("777")

    def setRecursivePermission(path: Path): Unit = {
      val listFiles = hdfs.listStatus(path)
      listFiles foreach { f =>
        val p = f.getPath
        hdfs.setPermission(p, rwx)
        if (f.isDirectory) setRecursivePermission(p)
      }
    }

    setRecursivePermission(hfilesPath)

    val lih = new LoadIncrementalHFiles(conf)
    val hbaseConn = hbaseUtil.getConnection
    // 根据表名获取表
    val table = hbaseConn.getTable(TableName.valueOf(tableName))
    // 获取hbase表的region分布
    //    val regionLocator = hbaseConn.getRegionLocator(TableName.valueOf(tableName))
    val job = Job.getInstance(conf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoadMap(job, table.asInstanceOf[HTable])

    // 开始导入
    lih.doBulkLoad(hfilesPath, table.asInstanceOf[HTable])
  }
}