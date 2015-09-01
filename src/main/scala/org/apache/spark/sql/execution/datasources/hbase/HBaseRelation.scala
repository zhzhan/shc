/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.hbase

import org.apache.avro.Schema
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.sources._
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConverters._
import scala.collection.mutable



/**
 * val people = sqlContext.read.format("hbase").load("people")
 */
private[sql] class DefaultSource extends RelationProvider {//with DataSourceRegister {

  //override def shortName(): String = "hbase"

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    HBaseRelation(parameters)(sqlContext)
  }
}

case class HBaseRelation(parameters: Map[String, String])(@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan with Logging {

  val tableCatalog = {
    HBaseTableCatalog(parameters)
  }
  val df: DataFrame = null
  @transient private var table_ : Option[Table] = None

  def conf = HBaseConfiguration.create

  def table = {
    table_.getOrElse{
      val connection = ConnectionFactory.createConnection(HBaseConfiguration.create)
      val t = connection.getTable(TableName.valueOf(tableCatalog.name))
      table_ = Some(t)
      t
    }
  }

  def closeTable() = {
    table_.map(_.close())
    table_ = None
  }

  // Return the key that can be used as partition keys, which satisfying two conditions:
  // 1: it has to be the row key
  // 2: it has to be sequentially sorted without gap in the row key
  def getRowColumns(c: Seq[Field]): Seq[Field] = {
    tableCatalog.getRowKey.zipWithIndex.filter { x =>
      c.contains(x._1)
    }.zipWithIndex.filter { x =>
      x._1._2 == x._2
    }.map(_._1._1)
  }

  def getProjections(requiredColumns: Array[String]): Seq[(Field, Int)] = {
    requiredColumns.map(tableCatalog.sMap.getField(_)).zipWithIndex
  }
  // Retrieve all columns we will return in the scanner
  def splitRowKeyColumns(requiredColumns: Array[String]): (Seq[Field], Seq[Field]) = {
    val (l, r) = requiredColumns.map(tableCatalog.sMap.getField(_)).partition(_.cf == HBaseTableCatalog.rowKey)

    (l, r)
  }

  override val schema: StructType = tableCatalog.toDataType

  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    HBaseTableScan(this, requiredColumns, filters).execute()
  }

  @transient lazy val partitions: Seq[HBasePartition] = {

    val connection = ConnectionFactory.createConnection(HBaseConfiguration.create)
    val r = connection.getRegionLocator(TableName.valueOf("t1"))
    val keys = r.getStartEndKeys
    keys.getFirst.zip(keys.getSecond)
      .zipWithIndex
      .map (x =>
        new HBasePartition(x._2,
          Some(x._1._1),
          Some(x._1._2),
          Some(r.getRegionLocation(x._1._1).getHostname)))
  }
}


// The definition of each column cell, which may be composite type
case class Field(
    cf: String,
    col: String,
    sType: Option[String] = None,
    avroSchema: Option[String] = None,
    val sedes: Option[Sedes[_]]= None) {
  def isRowKey = cf == HBaseTableCatalog.rowKey
  var start: Int = _
  val schema: Option[Schema] = avroSchema.map { x =>
    println(s"avro: $x")
    val p = new Schema.Parser
    p.parse(x)
  }
  def convertFunc: Option[Any => Any] = {
    schema.map(SchemaConverters.createConverterToSQL(_))
  }

  def toDataType = {
    sType.map(DataTypeParser.parse(_)).getOrElse{
      schema.map{ x=>
        SchemaConverters.toSqlType(x).dataType.asInstanceOf[StructType]
      }.get
    }
  }
  var length: Int = toDataType.defaultSize

}
// The row key definition, with each key refer to the col defined in Field, e.g.,
// key1:key2:key3
case class RowKey(keys: String) {
  def getKeys = keys.split(":")
  var fields: Seq[Field] = _
  var varLength = false
}
// The map between the column presented to Spark and the HBase field
case class SchemaMap(map: mutable.HashMap[String, Field]) {
  def toFileds = map.map { case (name, field) =>
    StructField(name, field.toDataType)
  }.toSeq

  def fields = map.values

  def getField(name: String) = map(name)
}
// The definition of HBase and Relation relation schema
case class TableCatalog(namespace: String, name: String, row: RowKey, sMap: SchemaMap) {
  def toDataType = StructType(sMap.toFileds)
  def getField(name: String) = sMap.getField(name)
  def getRowKey: Seq[Field] = row.getKeys.map(getField(_))

  def setupRowKeyMeta(rowKey: HBaseRawType) {
    if(row.varLength) {
      val f = row.getKeys.map(getField(_))
      var start = 0
      row.fields.zipWithIndex.foreach { f =>
        f._1.start = start
        if (f._1.toDataType == StringType) {
          var pos = rowKey.indexOf(HBaseTableCatalog.delimiter, f._2)
          if (pos == -1 || pos > rowKey.length) {
            // this is at the last dimension
            pos = rowKey.length
          }
          f._1.length = pos - start
        }
        start += f._1.length
      }
    }
  }
  def initRowKey = {
    val fields = sMap.fields.filter(_.cf == HBaseTableCatalog.rowKey)
    row.fields = row.getKeys.flatMap(n => fields.find(_.col == n))
    if (row.fields.filter(_.toDataType == StringType).isEmpty) {
      var start = 0
      row.fields.foreach { f =>
        f.start = start
        start += f.length
      }
    } else {
      row.varLength = true
    }
  }
  initRowKey
}

object HBaseTableCatalog {
  // The json string specifying hbase catalog information
  val tableCatalog = "catalog"
  // The row key with format key1:key2 specifying table row key
  val rowKey = "rowkey"
  // The key for hbase table whose value specify namespace and table name
  val table = "table"
  // The namespace of hbase table
  val nameSpace = "namespace"
  // The name of hbase table
  val tableName = "name"
  // The name of columns in hbase catalog
  val columns = "columns"
  val cf = "cf"
  val col = "col"
  val `type` = "type"
  // the name of avro schema json string
  val avro = "avro"
  val delimiter: Byte = 0
  val sedes = "sedes"
  /**
   * User provide table schema definition
   * {"tablename":"name", "rowkey":"key1:key2",
   * "columns":{"col1":{"cf":"cf1", "col":"col1", "type":"type1"},
   * "col2":{"cf":"cf2", "col":"col2", "type":"type2"}}}
   *  Note that any col in the rowKey, there has to be one corresponding col defined in columns
   */
  def apply(parameters: Map[String, String]): TableCatalog = {
  //  println(jString)
    val jString = parameters(tableCatalog)
    val map= parse(jString).values.asInstanceOf[Map[String,_]]
    val tableMeta = map.get(table).get.asInstanceOf[Map[String, _]]
    val nSpace = tableMeta.get(nameSpace).getOrElse("default").asInstanceOf[String]
    val tName = tableMeta.get(tableName).get.asInstanceOf[String]
    val cIter = map.get(columns).get.asInstanceOf[Map[String, Map[String, String]]].toIterator
    val schemaMap = mutable.HashMap.empty[String, Field]
    cIter.foreach { case (name, column)=>
      val sd = {
        column.get(sedes).asInstanceOf[Option[String]].map( n =>
          Class.forName(n).newInstance().asInstanceOf[Sedes[_]]
        )
      }
      val sAvro = column.get(avro).map(parameters(_))
      val f = Field(column.getOrElse(cf, rowKey),
        column.get(col).get,
        column.get(`type`),
        sAvro, sd)
      schemaMap.+= ((name, f))
    }
    val rKey = RowKey(map.get(rowKey).get.asInstanceOf[String])
    TableCatalog(nSpace, tName, rKey, SchemaMap(schemaMap))
  }

  def main(args: Array[String]) {


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
    val parameters = Map("schema1"->schema, tableCatalog->catalog)
    val t = HBaseTableCatalog(parameters)
    val d = t.toDataType
    println(d)

    val sqlContext: SQLContext = null
  }
}