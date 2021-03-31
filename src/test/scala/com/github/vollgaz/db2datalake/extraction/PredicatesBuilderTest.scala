package com.github.vollgaz.db2datalake.extraction
import java.sql.{Date, Timestamp}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.log4j.Logger
import org.apache.log4j.Level

class PredicatesBuilderTest extends AnyFlatSpec {

  val predBuilder = new PredicatesBuilder()

  "input check" should "return 1=1 for negative partition" in {
    val minmaxRow = Map("min" -> "0", "max" -> "10000")
    val partition = -150
    val result = predBuilder.apply(StructField("coltest", LongType), partition, minmaxRow)
    assert(result(0) == "1=1")
  }

  it should "return 1=1 if min is null" in {
    val minmaxRow = Map("min" -> null, "max" -> "10000")
    val partition = 1
    val result = predBuilder.apply(StructField("coltest", LongType), partition, minmaxRow)
    assert(result(0) == "1=1")
  }

  it should "return 1=1 if max is null" in {
    val minmaxRow = Map("min" -> "0", "max" -> null)
    val partition = 1
    val result = predBuilder.apply(StructField("coltest", LongType), partition, minmaxRow)
    assert(result(0) == "1=1")
  }

  it should "return 1=1 if max is null and min is null" in {
    val minmaxRow = Map("min" -> null, "max" -> null)
    val partition = 1
    val result = predBuilder.apply(StructField("coltest", LongType), partition, minmaxRow)
    assert(result(0) == "1=1")
  }

  "LongType" should "should return 1=1 for one partition for Long" in {
    val minmaxRow = Map("min" -> "0", "max" -> "10000")
    val partition = 1
    val result = predBuilder.apply(StructField("coltest", LongType), partition, minmaxRow)
    assert(result(0) == "1=1")
  }

  it should "return two predicates for 2 partitions for Long" in {
    val minmaxRow = Map("min" -> "0", "max" -> "10000")
    val partition = 2
    val result = predBuilder.apply(StructField("coltest", LongType), partition, minmaxRow)
    assert(result(0) == "coltest < 5000")
    assert(result(1) == "coltest >= 5000")
  }

  it should "return three predicates for 3 partitions for Long" in {
    val minmaxRow = Map("min" -> "0", "max" -> "9000")
    val partition = 3
    val result = predBuilder.apply(StructField("coltest", LongType), partition, minmaxRow)
    assert(result(0) == "coltest < 3000")
    assert(result(1) == "coltest >= 3000 AND coltest < 6000")
    assert(result(2) == "coltest >= 6000")
  }

  "TimestampType" should "should return 1=1 for one partition for java.sql.Timetstamp" in {
    val minmaxRow = Map("min" -> "2015-05-20 17:12:12.3332324", "max" -> "2019-12-31 12:12:12.9992324")
    val partition = 1
    val result = predBuilder.apply(StructField("coltest", TimestampType), partition, minmaxRow)
    assert(result(0) == "1=1")
  }

  it should "return two predicates for 2 partitions for java.sql.Timestamp" in {
    val minmaxRow = Map("min" -> "2015-05-20 17:12:12.3332324", "max" -> "2019-12-31 12:12:12.9992324")
    val partition = 2
    val result = predBuilder.apply(StructField("coltest", TimestampType), partition, minmaxRow)

    assert(result(0) == "coltest < '2017-09-09 15:12:12.666'")
    assert(result(1) == "coltest >= '2017-09-09 15:12:12.666'")
  }

  it should "return three predicates for 3 partitions for java.sql.Timestamp" in {
    val minmaxRow = Map("min" -> "2015-05-20 17:12:12.3332324", "max" -> "2019-12-31 12:12:12.9992324")
    val partition = 3
    val result = predBuilder.apply(StructField("coltest", TimestampType), partition, minmaxRow)
    assert(result(0) == "coltest < '2016-12-02 14:52:12.555'")
    assert(result(1) == "coltest >= '2016-12-02 14:52:12.555' AND coltest < '2018-06-17 14:32:12.777'")
    assert(result(2) == "coltest >= '2018-06-17 14:32:12.777'")
  }

  it should "work with timestamp from first century" in {
    val minmaxRow = Map("min" -> "0008-05-20 17:12:12.3332324", "max" -> "2019-12-31 12:12:12.9992324")
    val partition = 3
    val result = predBuilder.apply(StructField("coltest", TimestampType), partition, minmaxRow)
    assert(result(0) == "coltest < '0678-11-29 07:32:12.555'")
    assert(result(1) == "coltest >= '0678-11-29 07:32:12.555' AND coltest < '1349-06-08 21:52:12.777'")
    assert(result(2) == "coltest >= '1349-06-08 21:52:12.777'")
  }

  it should "work with timestamp of the end of time" in {
    val minmaxRow = Map("min" -> "0008-05-20 17:12:12.3332324", "max" -> "9999-12-31 24:59:59.999999999")
    val partition = 3
    val result = predBuilder.apply(StructField("coltest", TimestampType), partition, minmaxRow)
    assert(result(0) == "coltest < '3338-12-02 11:48:08.221'")
    assert(result(1) == "coltest >= '3338-12-02 11:48:08.221' AND coltest < '6669-06-17 07:24:04.109'")
    assert(result(2) == "coltest >= '6669-06-17 07:24:04.109'")
  }

  "Date" should "work with 3 partitions" in {
    val minmaxRow = Map("min" -> "1993-05-20", "max" -> "2020-02-25")
    val partition = 3
    val result = predBuilder.apply(StructField("coltest", DateType), partition, minmaxRow)
    assert(result(0) == "coltest < '2002-04-22'")
    assert(result(1) == "coltest >= '2002-04-22' AND coltest < '2011-03-24'")
    assert(result(2) == "coltest >= '2011-03-24'")
  }

  "Integer" should "work with 3 partitions" in {
    val minmaxRow = Map("min" -> "-999999", "max" -> "65464585")
    val partition = 3
    val result = predBuilder.apply(StructField("coltest", IntegerType), partition, minmaxRow)
    assert(result(0) == "coltest < 21154862")
    assert(result(1) == "coltest >= 21154862 AND coltest < 43309723")
    assert(result(2) == "coltest >= 43309723")
  }
  "Short" should "work with 3 partitions" in {
    val minmaxRow = Map("min" -> "-10", "max" -> "12")
    val partition = 3
    val result = predBuilder.apply(StructField("coltest", ShortType), partition, minmaxRow)
    assert(result(0) == "coltest < -3")
    assert(result(1) == "coltest >= -3 AND coltest < 4")
    assert(result(2) == "coltest >= 4")
  }

  "Decimal" should "work with 3 partitions" in {
    val minmaxRow = Map("min" -> "-999999999.999", "max" -> "10.264986416879189718")
    val partition = 3
    val result = predBuilder.apply(StructField("coltest", DecimalType(36, 18)), partition, minmaxRow)
    assert(result(0) == "coltest < -666666663.244337861040270094")
    assert(result(1) == "coltest >= -666666663.244337861040270094 AND coltest < -333333326.489675722080540188")
    assert(result(2) == "coltest >= -333333326.489675722080540188")
  }

  "Double" should "work with 3 partitions" in {
    val minmaxRow = Map("min" -> "-999999999.9491", "max" -> "10.651616")
    val partition = 3
    val result = predBuilder.apply(StructField("coltest", DoubleType), partition, minmaxRow)
    assert(result(0) == "coltest < -666666663.0821946666666666666666667")
    assert(result(1) == "coltest >= -666666663.0821946666666666666666667 AND coltest < -333333326.2152893333333333333333334")
    assert(result(2) == "coltest >= -333333326.2152893333333333333333334")
  }

  "Float" should "work with 3 partitions" in {
    val minmaxRow = Map("min" -> "10.65189", "max" -> "1894.651616")
    val partition = 3
    val result = predBuilder.apply(StructField("coltest", FloatType), partition, minmaxRow)
    assert(result(0) == "coltest < 638.65179697672526")
    assert(result(1) == "coltest >= 638.65179697672526 AND coltest < 1266.65170415242513")
    assert(result(2) == "coltest >= 1266.65170415242513")
  }

}
