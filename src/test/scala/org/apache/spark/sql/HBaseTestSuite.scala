package org.apache.spark.sql

import java.io.File

import com.google.common.io.Files
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, TableName, HBaseTestingUtility}
import org.apache.spark.Logging
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
import scala.collection.JavaConverters._

/**
 * Created by zzhang on 8/26/15.
 */
class HBaseTestSuite extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll  with Logging {
  private[spark] var htu = HBaseTestingUtility.createLocalHTU()
  private[spark] var tableName: Array[Byte] = Bytes.toBytes("t1")
  private[spark] var columnFamily: Array[Byte] = Bytes.toBytes("c")
  private[spark] var columnFamilies: Array[Array[Byte]] = Array(Bytes.toBytes("c"), Bytes.toBytes("d"))
 // private[spark] var columnFamilyStr = Bytes.toString(columnFamily)

  override def beforeAll() {
    val tempDir: File = Files.createTempDir
    tempDir.deleteOnExit
    htu.cleanupTestDir
    htu.startMiniZKCluster
    htu.startMiniHBaseCluster(1, 4)
    logInfo(" - minicluster started")
    println(" - minicluster started")
    try {
      htu.deleteTable(TableName.valueOf(tableName))

      //htu.createTable(TableName.valueOf(tableName), columnFamily, 2, Bytes.toBytes("abc"), Bytes.toBytes("xyz"), 2)
    } catch {
      case _ =>
        logInfo(" - no table " + Bytes.toString(tableName) + " found")
    }
  }



  override def afterAll() {
    try {
      println("shutdown")
      htu.deleteTable(TableName.valueOf(tableName))
      logInfo("shuting down minicluster")
      htu.shutdownMiniHBaseCluster
      htu.shutdownMiniZKCluster
      logInfo(" - minicluster shut down")
      htu.cleanupTestDir
    } catch {
      case _ => logError("teardown error")
    }
  }

  test("start to test HBase client") {
    val config = htu.getConfiguration
    htu.createMultiRegionTable(TableName.valueOf(tableName), columnFamilies)
    println("create htable t1")
    val connection = ConnectionFactory.createConnection(config)
    val r = connection.getRegionLocator(TableName.valueOf("t1"))
    val table = connection.getTable(TableName.valueOf("t1"))

    val regionLocations = r.getAllRegionLocations.asScala.toSeq
    println(s"$regionLocations size: ${regionLocations.size}")
    try {
      var put = new Put(Bytes.toBytes("delete1"))
      put.addColumn(columnFamily, Bytes.toBytes("a"), Bytes.toBytes("foo1"))
      table.put(put)
      put = new Put(Bytes.toBytes("delete2"))
      put.addColumn(columnFamily, Bytes.toBytes("a"), Bytes.toBytes("foo2"))
      table.put(put)
      put = new Put(Bytes.toBytes("delete3"))
      put.addColumn(columnFamily, Bytes.toBytes("a"), Bytes.toBytes("foo3"))
      table.put(put)
      println("htable put done")
    } finally {
      table.close()
    }
  }
}