package org.apache.spark.streaming.kafka

import kafka.serializer.StringDecoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificMutableRow
import org.apache.spark.sql.execution.datasources.hbase.{HBaseTableCatalog, SchemaConverters, AvroSedes}
import org.apache.spark.sql.types.{LongType, StructType, StructField}
import org.apache.spark.sql._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf, Logging}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange, KafkaUtils, KafkaTestUtils}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class AvroRecord(col0: Long,
    col1: Array[Byte])

object AvroRecord {
  val schemaString =
    s"""{"namespace": "example.avro",
        |   "type": "record",      "name": "User",
        |    "fields": [      {"name": "name", "type": "string"},
        |      {"name": "favorite_number",  "type": ["int", "null"]},
        |        {"name": "favorite_color", "type": ["string", "null"]}      ]    }""".stripMargin

  val avroSchema: Schema = {
    val p = new Schema.Parser
    p.parse(schemaString)
  }

  def apply(i: Long): AvroRecord = {

    val user = new GenericData.Record(avroSchema);
    user.put("name", s"name${"%03d".format(i)}")
    user.put("favorite_number", i)
    user.put("favorite_color", s"color${"%03d".format(i)}")
    val avroByte = AvroSedes.serialize(user, avroSchema)
    AvroRecord(i, avroByte)
  }
}

class StreamDFSuite extends SHC with Logging {
  private var kafkaTestUtils: KafkaTestUtils = new KafkaTestUtils
  val sparkConf = new SparkConf().setAppName("StreamDF")
  val sc = new SparkContext("local", "HBaseTest", sparkConf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  // for populating data where the avro is byte array.
  val insertCatalog = s"""{
        |"table":{"namespace":"default", "name":"avrotable"},
        |"rowkey":"key",
        |"columns":{
        |"timestamp":{"cf":"rowkey", "col":"key", "type":"long"},
        |"record":{"cf":"cf1", "col":"record", "type":"bytes"}
        |}
        |}""".stripMargin

  // read DF from table and write DF to table
  override val catalog = s"""{
      |"table":{"namespace":"default", "name":"avrotable"},
      |"rowkey":"key",
      |"columns":{
      |"timestamp":{"cf":"rowkey", "col":"key", "type":"long"},
      |"record":{"cf":"cf1", "col":"record", "avro":"avroSchema"}
      |}
      |}""".stripMargin

  def withCatalog(cat: String): DataFrame = {
    sqlContext
      .read
      .options(Map("avroSchema"->AvroRecord.schemaString, HBaseTableCatalog.tableCatalog->cat))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
  }

  val avroSchema: Schema = {
    val p = new Schema.Parser
    p.parse(AvroRecord.schemaString)
  }
  val st = SchemaConverters.toSqlType(avroSchema)
  val s = StructType(Seq(StructField("timestamp", LongType), StructField("record", st.dataType)))

  test("populate table") {
    //createTable(tableName, columnFamilies)
    import sqlContext.implicits._

    val data = (0L to 255L).map { i =>
      AvroRecord(i)
    }
    sc.parallelize(data).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> insertCatalog, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  test("Streaming with HBase") {

    val topics = Set("basic1", "basic2", "basic3")
    val data = Map("a" -> 7, "b" -> 9)
    topics.foreach { t =>
      kafkaTestUtils.createTopic(t)
      kafkaTestUtils.sendMessages(t, data)
    }
    val totalSent = data.values.sum * topics.size
    val kafkaParams = Map(
      "metadata.broker.list" -> kafkaTestUtils.brokerAddress,
      "auto.offset.reset" -> "smallest"
    )

    val ssc = new StreamingContext(sc, Seconds(2))
    var queryDF =  withCatalog(catalog)
    val stream =
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, topics)


    val allReceived =
      new ArrayBuffer[(String, String)] with mutable.SynchronizedBuffer[(String, String)]

    stream.foreachRDD { rdd =>
      val collected = rdd.mapPartitionsWithIndex { (i, iter) =>

        val row = new SpecificMutableRow(Seq(LongType, st.dataType))
        val newIter = new Iterator[InternalRow] {
          override def hasNext = {
            iter.hasNext
          }
          override def next: InternalRow = {
            val bytes = Bytes.toBytes(iter.next()._1)
            val record = AvroSedes.deserialize(bytes, avroSchema)
            val sqlRecord = SchemaConverters.createConverterToSQL(avroSchema)(record)
            row.setLong(0, 0L)
            row.update(1, sqlRecord)
            row
          }
        }
        newIter
      }.asInstanceOf[RDD[Row]]
      // create DF from stream and persist to table based on timestamp to achieve exactly once semantics
      val newDF = sqlContext.createDataFrame(collected, s)
      newDF.write.options(
        Map(HBaseTableCatalog.tableCatalog -> catalog))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()
      queryDF = queryDF.unionAll(newDF)
    }
    // do query on queryDF
    ssc.start()
    ssc.stop()
  }
}
