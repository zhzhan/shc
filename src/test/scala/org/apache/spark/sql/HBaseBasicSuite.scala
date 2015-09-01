package org.apache.spark.sql

import org.apache.spark.{SparkContext, SparkConf, Logging}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.spark.sql.SQLContext._
import scala.collection.JavaConversions._


import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql._
import org.apache.spark.sql.types._
/**
 * Created by zzhang on 8/27/15.
 */
class HBaseBasicSuite  extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll  with Logging{
  implicit class StringToColumn(val sc: StringContext) {
    def $(args: Any*): ColumnName = {
      new ColumnName(sc.s(args: _*))
    }
  }

  test("Basic setup") {

    val sc = new SparkContext("local", "HBaseTest")
    val sqlContext = new SQLContext(sc)

    val complex = s"""MAP<int, struct<varchar:string>>"""
    val schema =
      s"""{"namespace": "example.avro",
         |   "type": "record",      "name": "User",
         |    "fields": [      {"name": "name", "type": "string"},
         |      {"name": "favorite_number",  "type": ["int", "null"]},
         |        {"name": "favorite_color", "type": ["string", "null"]}      ]    }""".stripMargin

    val catalog = s"""{
            |"table":{"namespace":"default", "name":"htable"},
            |"rowkey":"key1:key2",
            |"columns":{
              |"col1":{"cf":"rowkey", "col":"key1", "type":"string"},
              |"col2":{"cf":"rowkey", "col":"key2", "type":"double"},
              |"col3":{"cf":"cf1", "col":"col1", "avro":"schema1"},
              |"col4":{"cf":"cf1", "col":"col2", "type":"binary"},
              |"col5":{"cf":"cf1", "col":"col3", "type":"double", "sedes":"org.apache.spark.sql.execution.datasources.hbase.DoubleSedes"},
              |"col6":{"cf":"cf1", "col":"col4", "type":"$complex"}
            |}
          |}""".stripMargin
    val df =
      sqlContext.read.options(Map("schema1"->schema, HBaseTableCatalog.tableCatalog->catalog)).format("org.apache.spark.sql.execution.datasources.hbase").load()
    val se = df.filter($"col2" > 12).filter($"col4" < Array(10.toByte)).select("col1")
    val se1 = df.filter($"col2" > 12).filter($"col4" < Array(10.toByte)).select("col1")
    se.count()
    se1.collect.foreach(println(_))
    println(df)


  }
}
