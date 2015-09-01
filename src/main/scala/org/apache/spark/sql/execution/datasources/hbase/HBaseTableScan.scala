package org.apache.spark.sql.execution.datasources.hbase

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratePredicate
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.AtomicType

import scala.collection.JavaConverters._

import org.apache.hadoop.hbase.{CellUtil, Cell, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Result, ResultScanner, Scan, HTable}
import org.apache.hadoop.hbase.regionserver.RegionScanner
import org.apache.spark.{InterruptibleIterator, TaskContext, Partition, Logging}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.sources.Filter

private[hbase] case class HBaseTableScan(
    relation: HBaseRelation,
    requiredColumns: Array[String],
    filters: Array[Filter]) {

  def execute(): RDD[Row] = {
    new HBaseReaderRDD(relation, requiredColumns, filters)
  }
}


private[hbase] class HBasePartition(
    override val index: Int,
    val start: Option[HBaseRawType] = None,
    val end: Option[HBaseRawType] = None,
    val server: Option[String] = None) extends Partition


private[hbase] class HBaseReaderRDD(
    relation: HBaseRelation,
    requiredColumns: Array[String],
    filters: Array[Filter]) extends RDD[Row](relation.sqlContext.sparkContext, Nil) with Logging  {

  filters.foreach(println(_))

  override def getPartitions: Array[Partition] = relation.partitions.toArray


  def buildRow(
      fieldsProj: Seq[(Field, Int)],
      result: Result,
      row: MutableRow) = {
    val r = result.getRow
    relation.tableCatalog.setupRowKeyMeta(r)
    fieldsProj.map { x =>
      if (x._1.isRowKey) {
        if (x._1.start + x._1.length <= r.length) {
          Utils.setRowCol(row, x, r, x._1.start, x._1.length)
        } else {
          row.setNullAt(x._2)
        }
      } else {
        val kv = result.getColumnLatestCell(Bytes.toBytes(x._1.cf), Bytes.toBytes(x._1.col))
        if (kv == null || kv.getValueLength == 0) {
          row.setNullAt(x._2)
        } else if (x._1.toDataType.isInstanceOf[AtomicType]) {
          val v = CellUtil.cloneValue(kv)
          Utils.setRowCol(row, x, v, 0, v.length)
        }
      }
    }
  }

  private def toIterator(
      context: TaskContext,
      scanner: ResultScanner): Iterator[Row] = {
    val row = new GenericMutableRow(requiredColumns.size)

    val iterator = new Iterator[Row] {
      var result: Option[Result] = None
      val fieldsProj = relation.getProjections(requiredColumns)
      override def hasNext: Boolean = {
        if (result.isEmpty) {
          val r = scanner.next
          if (r == null) {
            result = None
            close()
          } else {
            result = Some(r)
          }
        }
        result.map(x => true).getOrElse(false)
      }

      override def next(): Row = {
        if (hasNext) {
          buildRow(fieldsProj, result.get, row)
          row
        } else {
          null
        }
      }

      def close() = {
        try {
          scanner.close()
          relation.closeHTable()
        } catch {
          case e: Exception => logWarning("Exception in scanner.close", e)
        }
      }
    }
    iterator
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[HBasePartition].server.map {
      identity
    }.toSeq
  }

  private def buildScan(
      start: Option[HBaseRawType],
      end: Option[HBaseRawType],
      columns: Seq[Field]): Scan = {
    val scan = {
      (start, end) match {
        case (Some(lb), Some(ub)) => new Scan(lb, ub)
        case (Some(lb), None) => new Scan(lb)
        case (None, Some(ub)) => new Scan(Array[Byte](), ub)
        case _ => new Scan
      }
    }

    // set fetch size
    // scan.setCaching(scannerFetchSize)
    columns.foreach{ c =>
      scan.addColumn(Bytes.toBytes(c.cf), Bytes.toBytes(c.col))
    }
    scan
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val partition = split.asInstanceOf[HBasePartition]
    val scan = buildScan(partition.start, partition.end, relation.splitRowKeyColumns(requiredColumns)._2)
    val scanner = relation.hTable.getScanner(scan)
    toIterator(context, scanner)
  }
}