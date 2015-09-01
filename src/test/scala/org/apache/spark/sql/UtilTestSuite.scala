package org.apache.spark.sql

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Put, ConnectionFactory}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.Logging
import org.apache.spark.sql.execution.datasources.hbase.Utils
import org.apache.spark.sql.types.BinaryType
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

/**
 * Created by zzhang on 8/27/15.
 */
class UtilTestSuite extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll  with Logging{
  val order =  BinaryType.ordering
  test("Test range 1") {
    val v = Utils.findRanges(50, 100, Array((0, 52), (80, 120)))
    assert(v(0)._1 == 50 && v(0)._2 == 52 &&
      v(1)._1 == 80 && v(1)._2 == 100 )
  }

  test("Test range 2") {
    val v = Utils.findRanges(50, 100, Array((52, 80)))
    assert(v(0)._1 == 52 && v(0)._2 == 80)
  }

  test("Testrange 3") {
    val v = Utils.findRanges(50, 100, Array((0, 50), (100, 120)))
    assert(v.size == 0)
  }

  test("Test range 4") {
    val v = Utils.findRanges(50, 100, Array((0, 50), (80, 120)))
    assert(v(0)._1 == 80 && v(0)._2 == 100)
  }

  test("Test range 5") {
    val v = Utils.findRanges(50, 100, Array((0, 40), (110, 120)))
    assert(v.size == 0)
  }

  test("Testrange 6") {
    val v = Utils.findRanges(Array(50.toByte, 20.toByte), Array(100.toByte, 100.toByte),
      Array((Array(0.toByte), Array(50.toByte, 20.toByte)), (Array(100.toByte, 100.toByte), Array(120.toByte))))(order)
    assert(v.size == 0)
  }

  test("Test range 7") {
    val v = Utils.findRanges(Array(50.toByte), Array(100.toByte),
      Array((Array(0.toByte), Array(50.toByte)), (Array(80.toByte), Array(120.toByte))))(order)
    assert(order.compare(v(0)._1, Array(80.toByte)) == 0 &&
      order.compare(v(0)._2, Array(100.toByte)) == 0)
  }
}
