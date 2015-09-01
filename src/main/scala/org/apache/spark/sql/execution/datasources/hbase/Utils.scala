package org.apache.spark.sql.execution.datasources.hbase

import java.util
import java.util.Comparator

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.catalyst.expressions.MutableRow
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.SparkSqlSerializer
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer
import scala.math.Ordering


/**
 * Created by zzhang on 8/18/15.
 */
object Utils {

  def setRowCol(
      row: MutableRow,
      field: (Field, Int),
      src: HBaseRawType,
      offset: Int,
      length: Int): Unit = {
    val index = field._2
    field._1.sedes.map{ s =>
      val m = s.deserialize(src, offset, length)
      // The convertFunc will convert avro record to sql type
      val n = field._1.convertFunc.map(_(m))
      row.update(index, n)
    }.getOrElse {
      field._1.toDataType match {
        case BooleanType => row.setBoolean(index, toBoolean(src, offset))
        case ByteType => row.setByte(index, toByte(src, offset))
        case DoubleType => row.setDouble(index, toDouble(src, offset))
        case FloatType => row.setFloat(index, toFloat(src, offset))
        case IntegerType => row.setInt(index, toInt(src, offset))
        case LongType => row.setLong(index, toLong(src, offset))
        case ShortType => row.setShort(index, toShort(src, offset))
        case StringType => row.update(index, toUTF8String(src, offset, length))
        case _ => row.update(index, SparkSqlSerializer.deserialize[Any](src)) //TODO
      }
    }
  }

  def toBoolean(input: HBaseRawType, offset: Int): Boolean = {
    input(offset) != 0
  }

  def toByte(input: HBaseRawType, offset: Int): Byte = {
    // Flip sign bit back
    val v: Int = input(offset) ^ 0x80
    v.asInstanceOf[Byte]
  }

  def toDouble(input: HBaseRawType, offset: Int): Double = {
    var l: Long = Bytes.toLong(input, offset, Bytes.SIZEOF_DOUBLE)
    l = l - 1
    l ^= (~l >> java.lang.Long.SIZE - 1) | java.lang.Long.MIN_VALUE
    java.lang.Double.longBitsToDouble(l)
  }
  def toFloat(input: HBaseRawType, offset: Int): Float = {
    var i = Bytes.toInt(input, offset)
    i = i - 1
    i ^= (~i >> Integer.SIZE - 1) | Integer.MIN_VALUE
    java.lang.Float.intBitsToFloat(i)
  }

  def toInt(input: HBaseRawType, offset: Int): Int = {
    // Flip sign bit back
    var v: Int = input(offset) ^ 0x80
    for (i <- 1 to Bytes.SIZEOF_INT - 1) {
      v = (v << 8) + (input(i + offset) & 0xff)
    }
    v
  }

  def toLong(input: HBaseRawType, offset: Int): Long = {
    // Flip sign bit back
    var v: Long = input(offset) ^ 0x80
    for (i <- 1 to Bytes.SIZEOF_LONG - 1) {
      v = (v << 8) + (input(i + offset) & 0xff)
    }
    v
  }

  def toShort(input: HBaseRawType, offset: Int): Short = {
    // flip sign bit back
    var v: Int = input(offset) ^ 0x80
    v = (v << 8) + (input(1 + offset) & 0xff)
    v.asInstanceOf[Short]
  }

  def toUTF8String(input: HBaseRawType, offset: Int, length: Int): UTF8String = {
    UTF8String(input.slice(offset, offset + length))
  }

  // We assume that start is inclusive, and end is  exclusive. This is applied to the input ranges as well.
  // If the end is inclusive, it is the caller's responsibility to make it inclusive, for example
  // append a byte at the end of the end. It is ok not to be accurate,
  // since the upper layer will do the filtering anyway
  // For Array[Byte] we use val c =  BinaryType.ordering
  def findRanges[T](
     start: T,
     end: T,
     ranges: Array[(T, T)])(implicit ordering: Ordering[T]): (Array[(T, T)]) = {
    val search = new ArrayBuffer[(T, T)]()
    // we assume that the range size is reasonably small.
    ranges.foreach { case (rs, re) =>
      var l =  if (ordering.compare(start, rs) >= 0) start else rs
      val r = if (ordering.compare(end, re) <= 0)  end else re
      if (ordering.compare(l, r) < 0) {
        search += ((l, r))
      }
    }
    search.toArray
  }
}
