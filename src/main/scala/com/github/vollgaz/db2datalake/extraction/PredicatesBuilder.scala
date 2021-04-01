package com.github.vollgaz.db2datalake.extraction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import java.sql.Timestamp
import java.sql.Date
import scala.math.BigDecimal

/** Spark 2.x can only handle numerical column in partitioning over JDBC.
  * This class is here to partially patch this behavior by creating a list of predicate for splitting the download.
  */
class PredicatesBuilder {

  def apply(column: StructField, numPartitions: Int, minmax: Map[String, String]): Array[String] = {
    if (minmax("min") == null || minmax("max") == null || numPartitions <= 1) return Array[String]("1=1")
    column.dataType match {
      case _: TimestampType =>
        val pivotValues: Array[Any] =
          splitOnLong(numPartitions, Timestamp.valueOf(minmax("min")).getTime(), Timestamp.valueOf(minmax("max")).getTime())
            .map(x => new java.sql.Timestamp(x))
            .map(x => s"""'$x'""")
        buildPredicates(column.name, pivotValues)
      case _: DateType =>
        val pivotValues: Array[Any] = splitOnLong(numPartitions, Date.valueOf(minmax("min")).getTime(), Date.valueOf(minmax("max")).getTime())
          .map(x => new java.sql.Date(x))
          .map(x => s"""'$x'""")
        buildPredicates(column.name, pivotValues)
      case _: LongType =>
        val pivotValues = splitOnLong(numPartitions, minmax("min").toLong, minmax("max").toLong).toArray[Any]
        buildPredicates(column.name, pivotValues)
      case _: IntegerType =>
        val pivotValues = splitOnLong(numPartitions, minmax("min").toInt, minmax("max").toInt).toArray[Any]
        buildPredicates(column.name, pivotValues)
      case _: ShortType =>
        val pivotValues = splitOnLong(numPartitions, minmax("min").toShort, minmax("max").toShort).toArray[Any]
        buildPredicates(column.name, pivotValues)
      case _: ByteType =>
        val pivotValues = splitOnLong(numPartitions, minmax("min").toByte, minmax("max").toByte).toArray[Any]
        buildPredicates(column.name, pivotValues)
      case _: DecimalType =>
        val pivotValues = splitOnBigDecimal(numPartitions, BigDecimal(minmax("min")), BigDecimal(minmax("max"))).toArray[Any]
        buildPredicates(column.name, pivotValues)
      case _: DoubleType =>
        val pivotValues = splitOnBigDecimal(numPartitions, minmax("min").toDouble, minmax("max").toDouble).toArray[Any]
        buildPredicates(column.name, pivotValues)
      case _: FloatType =>
        val pivotValues = splitOnBigDecimal(numPartitions, minmax("min").toFloat, minmax("max").toFloat).toArray[Any]
        buildPredicates(column.name, pivotValues)
      // Will download everything on one partition
      case _ => Array[String]("1=1")
    }
  }

  /** Calculate the intermediaries values on the space between min and max in function of the number of partition
    *
    * @param numPartition
    * @param min
    * @param max
    * @return
    */
  private def splitOnBigDecimal(numPartition: Int, min: scala.math.BigDecimal, max: scala.math.BigDecimal): Array[BigDecimal] = {
    val increment: BigDecimal = (max - min) / numPartition
    Range(1, numPartition, 1).map(x => min + (increment * x)).toArray[scala.math.BigDecimal]
  }

  /** Calculate the intermediaries values on the space between min and max in function of the number of partition
    *
    * @param numPartition
    * @param min
    * @param max
    * @return
    */
  private def splitOnLong(numPartition: Int, min: Long, max: Long): Array[Long] = {
    val increment: Long = (max - min) / numPartition
    Range(1, numPartition, 1).map(x => min + (increment * x)).toArray
  }

  /** Create the predicates for querying the data over sql with spark
    * The lowest range is defined by lesser than the first pivotValue.
    * The highest range is defined by greater the last pivotValue.
    *
    * @param colName     The names of the column used for the repartition.
    * @param pivotValues The values separating ranges
    *                    example : we have min = 10 and max = 90 with 4 partitions
    *                    the pivotValues are 30 , 50 , 70
    *                    partition 1 : X < 30
    *                    partition 2 : 30 <= X < 50
    *                    partition 3 : 50 <= X < 70
    *                    partition 4 : 70 <= X
    * @return The list of predicates for splitting the data download.
    */
  def buildPredicates(colName: String, pivotValues: Array[Any]): Array[String] = {
    if (pivotValues.length == 1) Array[String](s"$colName < ${pivotValues(0).toString}", s"$colName >= ${pivotValues(0).toString}")
    else {
      val lowestRange = Array[String](s"$colName < ${pivotValues(0).toString}")
      val midRanges: Seq[String] = Range(0, pivotValues.length - 1, 1)
        .map(index => s"$colName >= ${pivotValues(index).toString} AND $colName < ${pivotValues(index + 1).toString}")
      val highestRange = Array[String](s"$colName >= ${pivotValues(pivotValues.length - 1).toString}")
      lowestRange ++ midRanges ++ highestRange
    }
  }
}
