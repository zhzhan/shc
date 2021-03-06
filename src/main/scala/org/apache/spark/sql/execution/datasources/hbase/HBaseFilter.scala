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

import java.util

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{Filter => HFilter, FilterList => HFilterList, _}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.Logging
import org.apache.spark.sql.execution.datasources.hbase
import org.apache.spark.sql.execution.datasources.hbase.FilterType.FilterType
import org.apache.spark.sql.execution.datasources.hbase.TypedFilter
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.BinaryType
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.math.Ordering
import scala.reflect.ClassTag

object FilterType extends Enumeration {
  type FilterType = Value
  val And, Or, Atomic, Prefix, Und = Value
  def getOperator(hType: FilterType): HFilterList.Operator = hType match {
    case And => HFilterList.Operator.MUST_PASS_ALL
    case Or => HFilterList.Operator.MUST_PASS_ONE
  }
  def parseFrom(fType: FilterType): Array[Byte] => HFilter = fType match {
    case And | Or => {
      x: Array[Byte] => HFilterList.parseFrom(x).asInstanceOf[HFilter]
    }
    case Atomic  => {
      x: Array[Byte] => SingleColumnValueFilter.parseFrom(x).asInstanceOf[HFilter]
    }
    case Prefix =>  {
      x: Array[Byte] => PrefixFilter.parseFrom(x).asInstanceOf[HFilter]
    }
    case Und => throw new Exception("unknown type")
  }
}

case class TypedFilter(filter: Option[HFilter], hType: FilterType)

case class SerializedTypedFilter(b: Option[Array[Byte]], hType: FilterType)

object TypedFilter {
  def toSerializedTypedFilter(tf: TypedFilter): SerializedTypedFilter = {
    val b = tf.filter.map(_.toByteArray)
    SerializedTypedFilter(b, tf.hType)
  }

  def fromSerializedTypedFilter(tf: SerializedTypedFilter): TypedFilter = {
    val filter = tf.b.map(x => FilterType.parseFrom(tf.hType)(x))
    TypedFilter(filter, tf.hType)
  }

  def empty = TypedFilter(None, FilterType.Und)

  private def getOne(left: TypedFilter, right: TypedFilter) = {
    if (left.filter.isEmpty) {
      right
    } else {
      left
    }
  }

  private def ops(left: TypedFilter, right: TypedFilter, hType: FilterType) = {
    if (left.hType == hType) {
      val l = left.filter.get.asInstanceOf[HFilterList]
      if (right.hType == hType) {
        val r = right.filter.get.asInstanceOf[HFilterList].getFilters
        r.foreach(l.addFilter(_))
      } else {
        l.addFilter(right.filter.get)
      }
      left
    } else if (right.hType == hType) {
      val r = right.filter.get.asInstanceOf[HFilterList]
      r.addFilter(left.filter.get)
      right
    } else {
      val nf = new HFilterList(FilterType.getOperator(hType))
      nf.addFilter(left.filter.get)
      nf.addFilter(right.filter.get)
      TypedFilter(Some(nf), hType)
    }
  }

  def and(left: TypedFilter, right: TypedFilter): TypedFilter = {
    if (left.filter.isEmpty) {
      right
    } else if (right.filter.isEmpty) {
      left
    } else {
      ops(left, right, FilterType.And)
    }
  }
  def or(left: TypedFilter, right: TypedFilter): TypedFilter = {
    if (left.filter.isEmpty || right.filter.isEmpty) {
      TypedFilter.empty
    } else {
      ops(left, right, FilterType.Or)
    }
  }
}

// Combination of HBase range and filters
case class HRF[T](ranges: Array[ScanRange[T]], tf: TypedFilter)

object HRF {
  def empty[T] = HRF[T](Array(ScanRange.empty[T]), TypedFilter.empty)
}

object HBaseFilter extends Logging{
  implicit val order: Ordering[Array[Byte]] =  hbase.ord
  def buildFilters(filters: Array[Filter], relation: HBaseRelation): HRF[Array[Byte]] = {
    if (log.isDebugEnabled) {
      logDebug(s"for all filters: ")
      filters.foreach(x => logDebug(x.toString))
    }
    val ret =
      filters.map(x=> buildFilter(x, relation)).reduceOption[HRF[Array[Byte]]] { case (x, y) =>
        and[Array[Byte]](x, y)
      }.getOrElse(HRF.empty[Array[Byte]])
    if (log.isDebugEnabled) {
      logDebug("ret:")
      ret.ranges.foreach(x => logDebug(x.toString))
    }
    ret
  }

  private def toBytes[T](value: T, att: String, relation: HBaseRelation): Array[Byte] = {
    Utils.toBytes(value, relation.getField(att))
  }


  def process(value: Any, relation: HBaseRelation, attribute: String,
      primary: BoundRanges => HRF[Array[Byte]],
      column: BoundRanges => HRF[Array[Byte]],
      composite:  BoundRanges => HRF[Array[Byte]]): HRF[Array[Byte]] = {
    val b = BoundRange(value)
    val ret: Option[HRF[Array[Byte]]] = {
      if (relation.isPrimaryKey(attribute)) {
        b.map(primary(_))
      } else if (relation.isColumn(attribute)) {
        b.map(column(_))
      } else {
        Some(HRF.empty[Array[Byte]])
        // composite key does not work, need more work
        /*
        if (!relation.rows.varLength) {
          b.map(composite(_))
        } else {
          None
        }*/
      }
    }
    ret.getOrElse(HRF.empty[Array[Byte]])
  }

  def buildFilter(filter: Filter, relation: HBaseRelation): HRF[Array[Byte]] = {
    // We treat greater and greaterOrEqual as the same
    def Greater(attribute: String, value: Any): HRF[Array[Byte]]  = {
        process(value, relation, attribute,
        bound => {
          if (relation.singleKey) {
            HRF(bound.greater.map(x =>
              ScanRange(Some(Bound(x.low, true)),
                Some(Bound(x.upper, true)))), TypedFilter.empty)
          } else {
            val s = bound.greater.map( x=>
              ScanRange(relation.rows.length,
                x.low, true, x.upper, true, relation.getField(attribute).start))
            HRF(s, TypedFilter.empty)
          }
        },
        bound => {
          val f = relation.getField(attribute)
          val filter = bound.greater.map { x =>
            val lower = new SingleColumnValueFilter(
              Bytes.toBytes(f.cf),
              Bytes.toBytes(f.col),
              CompareOp.GREATER_OR_EQUAL,
              x.low)
            val low = TypedFilter(Some(lower), FilterType.Atomic)
            val upper = new SingleColumnValueFilter(
              Bytes.toBytes(f.cf),
              Bytes.toBytes(f.col),
              CompareOp.LESS_OR_EQUAL,
              x.upper)
            val up = TypedFilter(Some(upper), FilterType.Atomic)
            TypedFilter.and(low, up)
          }
          val of = filter.reduce[TypedFilter]{ case (x, y) =>
            TypedFilter.or(x, y)}
          HRF(Array(ScanRange.empty[Array[Byte]]), of)
        },
        bound => {
          val s = bound.greater.map( x=>
            ScanRange(relation.rows.length,
              x.low, true, x.upper, true, relation.getField(attribute).start))
          HRF(s, TypedFilter.empty)
        })
    }
    // We treat less and lessOrEqual as the same
    def Less(attribute: String, value: Any): HRF[Array[Byte]]  = {
      process(value, relation, attribute,
        bound => {
          if (relation.singleKey) {
            HRF(bound.less.map(x =>
              ScanRange(Some(Bound(x.low, true)),
                Some(Bound(x.upper, true)))), TypedFilter.empty)
          } else {
            val s = bound.less.map( x=>
              ScanRange(relation.rows.length,
                x.low, true, x.upper, true, relation.getField(attribute).start))
            HRF(s, TypedFilter.empty)
          }
        },
        bound => {
          val f = relation.getField(attribute)
          val filter = bound.less.map { x =>
            val lower = new SingleColumnValueFilter(
              Bytes.toBytes(f.cf),
              Bytes.toBytes(f.col),
              CompareOp.GREATER_OR_EQUAL,
              x.low)
            val low = TypedFilter(Some(lower), FilterType.Atomic)
            val upper = new SingleColumnValueFilter(
              Bytes.toBytes(f.cf),
              Bytes.toBytes(f.col),
              CompareOp.LESS_OR_EQUAL,
              x.upper)
            val up = TypedFilter(Some(upper), FilterType.Atomic)
            TypedFilter.and(low, up)
          }
          val ob = filter.reduce[TypedFilter]{ case (x, y) =>
            TypedFilter.or(x, y)}
          HRF(Array(ScanRange.empty[Array[Byte]]), ob)
        },
        bound => {
          val s = bound.less.map( x=>
            ScanRange(relation.rows.length,
              x.low, true, x.upper, true, relation.getField(attribute).start))
          HRF(s, TypedFilter.empty)
        })
    }

    val f = filter match {
      case And(left, right) =>
        and[Array[Byte]](buildFilter(left, relation), buildFilter(right, relation))
      case Or(left, right) =>
        or[Array[Byte]](buildFilter(left, relation), buildFilter(right, relation))
      case EqualTo(attribute, value) =>
        process(value, relation, attribute,
          bound => {
            if (relation.singleKey) {
              HRF(Array(ScanRange(Some(Bound(bound.value, true)),
                Some(Bound(bound.value, true)))), TypedFilter.empty)
            } else {
              val s = ScanRange(relation.rows.length,
                bound.value, true, bound.value, true, relation.getField(attribute).start)
              HRF(Array(s), TypedFilter.empty)
            }
          },
          bound => {
            val f = relation.getField(attribute)
            val filter = new SingleColumnValueFilter(
              Bytes.toBytes(f.cf),
              Bytes.toBytes(f.col),
              CompareOp.EQUAL,
              bound.value)
            HRF(Array(ScanRange.empty[Array[Byte]]), TypedFilter(Some(filter), FilterType.Atomic))
          },
          bound => {
            val s = ScanRange(relation.rows.length,
              bound.value, true, bound.value, true, relation.getField(attribute).start)
            HRF(Array(s), TypedFilter.empty)
          })
      case LessThan(attribute, value) =>
        Less(attribute, value)
      case LessThanOrEqual(attribute, value)  =>
        Less(attribute, value)
      case GreaterThan(attribute, value) =>
        Greater(attribute, value)
      case GreaterThanOrEqual(attribute, value)  =>
        Greater(attribute, value)
      case  StringStartsWith(attribute, value)  =>
        val b = Bytes.toBytes(value)
        if (relation.isPrimaryKey(attribute)) {
          val prefixFilter = new PrefixFilter(b)
          HRF(Array(ScanRange.empty[Array[Byte]]),
            TypedFilter(Some(prefixFilter), FilterType.Prefix))
        } else if (relation.isColumn(attribute)) {
          val f = relation.getField(attribute)
          val filter = new SingleColumnValueFilter(
            Bytes.toBytes(f.cf),
            Bytes.toBytes(f.col),
            CompareOp.EQUAL,
            new BinaryPrefixComparator(b)
          )
          HRF(Array(ScanRange.empty[Array[Byte]]), TypedFilter(Some(filter), FilterType.Atomic))
        } else {
          HRF.empty[Array[Byte]]
        }

      case  StringEndsWith(attribute, value) if (relation.isColumn(attribute)) =>
        val f = relation.getField(attribute)
        val filter = new SingleColumnValueFilter(
          Bytes.toBytes(f.cf),
          Bytes.toBytes(f.col),
          CompareOp.EQUAL,
          new RegexStringComparator(s".*$value")
        )
        HRF(Array(ScanRange.empty[Array[Byte]]), TypedFilter(Some(filter), FilterType.Atomic))

      case StringContains(attribute: String, value: String) if relation.isColumn(attribute) =>
        val f = relation.getField(attribute)
        val filter = new SingleColumnValueFilter(
          Bytes.toBytes(f.cf),
          Bytes.toBytes(f.col),
          CompareOp.EQUAL,
          new SubstringComparator(value)
        )
        HRF(Array(ScanRange.empty[Array[Byte]]), TypedFilter(Some(filter), FilterType.Atomic))
      // We should also add Not(GreatThan, LessThan, ...)
      // because if we miss some filter, it may result in a large scan range.
      case Not(StringContains(attribute: String, value: String)) if relation.isColumn(attribute) =>
        val b = Bytes.toBytes(value)
        val f = relation.getField(attribute)
        val filter = new SingleColumnValueFilter(
          Bytes.toBytes(f.cf),
          Bytes.toBytes(f.col),
          CompareOp.NOT_EQUAL,
          new SubstringComparator(value)
        )
        HRF(Array(ScanRange.empty[Array[Byte]]), TypedFilter(Some(filter), FilterType.Atomic))

      case _ => HRF.empty[Array[Byte]]
    }

    if (log.isDebugEnabled) {
      logDebug(s"start filter $filter")
      f.ranges.foreach(x => logDebug(x.toString))
    }
    f
  }

  def and[T](
      left: HRF[T],
      right: HRF[T])(implicit ordering: Ordering[T]):HRF[T] = {
    // (0, 5), (10, 15) and with (2, 3) (8, 12) = (2, 3), (10, 12)
    val tmp = left.ranges.map(x => ScanRange.and(x, right.ranges))
    val ranges = ScanRange.and(left.ranges, right.ranges)
    val typeFilter = TypedFilter.and(left.tf, right.tf)
    HRF(ranges, typeFilter)
  }

  def or[T](
      left: HRF[T],
      right: HRF[T])(implicit ordering: Ordering[T]):HRF[T] = {
    val ranges = ScanRange.or(left.ranges, right.ranges)
    val typeFilter = TypedFilter.or(left.tf, right.tf)
    HRF(ranges, TypedFilter.empty)
  }
}
