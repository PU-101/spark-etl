name := "spark-etl"

version := "0.1"

scalaVersion := "2.11.12"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.25",
  "org.slf4j" % "slf4j-log4j12" % "1.7.25",
  "com.typesafe" % "config" % "1.3.2",
  "org.apache.hbase" % "hbase-common" % "1.2.0",
  "org.apache.hbase" % "hbase-client" % "1.2.0",
  "org.apache.hbase" % "hbase-server" % "1.2.0",
  "org.apache.hbase" % "hbase-hadoop-compat" % "1.2.0",
  "org.apache.spark" % "spark-core_2.11" % "2.2.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.2.0",
  "org.apache.spark" % "spark-hive_2.11" % "2.2.0")

mainClass in Compile := Some("com.yxt.bigdata.etl.MainHandler")
