package com.github.vollgaz.db2datalake.extraction
import java.util.Properties

import org.apache.spark.sql.{Row, SparkSession}

import scala.util.matching.Regex

case class ExtractionPlan(table: String, predicates: Array[String])

object ExtractionPlan {
  val regRange: Regex = """^\[(.*)\]$""".r
  val regStep: Regex = """^\](.*)\[$""".r
  val regNumPart: Regex = """^\{(.*)\}$""".r
  val predicatesBuilder = new PredicatesBuilder()
  val spark: SparkSession = SparkSession.builder().getOrCreate()

  /** From a configuration string define how the data should be split throught differents jdbc connexion
    *
    * @param tableConfig define how to partition the extraction.
    *                    - 'mytable'
    *                    - 'mytable::splitColumn'
    *                    - 'mytable::splitColumn::[step1~step2~...~stepN]'
    *                    - 'mytable::splitColumn::[min~max]'
    *                    - 'mytable::splitColumn::[min~max~numberPartitions]'
    *                    - 'mytable::splitColumn::{numberPartitions}'
    * @return
    */
  def fromString(sparkJdbcProperties: Map[String, String], tableConfig: String, defaultNumPartitions: Int): ExtractionPlan = {
    tableConfig.split("::") match {
      case Array(table, column, config) =>
        config match {
          case regStep(steps)           => predicatesWithProvidedSteps(table, column, steps)
          case regNumPart(numPartition) => predicatesOverJdbc(sparkJdbcProperties, numPartition.toInt, table, column)
          case regRange(rangeConf)      => predicatesOverJdbcWithRange(sparkJdbcProperties, defaultNumPartitions, table, column, rangeConf)
        }
      case Array(t, c) => predicatesOverJdbc(sparkJdbcProperties, defaultNumPartitions, t, c)
      case Array(t)    => predicatesOverJdbc(sparkJdbcProperties, defaultNumPartitions, t)
      // TODO : Found proper exception
      case _ => throw new Exception(s"table configuration uninterpretable -- config:$tableConfig")
    }
  }

  /** @param table
    * @param column
    * @param stepsConf
    * @return
    */
  def predicatesWithProvidedSteps(table: String, column: String, stepsConf: String): ExtractionPlan = {
    ExtractionPlan(table, predicatesBuilder.buildPredicates(column, stepsConf.split('~').toArray[Any]))
  }

  /** @param sparkJdbcProperties
    * @param numPartitions
    * @param table
    * @param column
    * @param rangeConf
    * @return
    */
  def predicatesOverJdbcWithRange(
      sparkJdbcProperties: Map[String, String],
      numPartitions: Int,
      table: String,
      column: String,
      rangeConf: String
  ): ExtractionPlan = {

    val col = spark.read
      .format("jdbc")
      .options(sparkJdbcProperties)
      .option("dbTable", table)
      .load()
      .schema(column)

    rangeConf.split("~") match {
      case Array(min, max, numPartitions) =>
        val limits = limitValuesOnRange(sparkJdbcProperties, table, col.name, min, max)
        ExtractionPlan(table, predicatesBuilder(col, numPartitions.toInt, limits))
      case Array(min, max) =>
        val limits = limitValuesOnRange(sparkJdbcProperties, table, col.name, min, max)
        ExtractionPlan(table, predicatesBuilder(col, numPartitions, limits))
    }
  }

  /** @param sparkJdbcProperties
    * @param table
    * @param colName
    * @param min
    * @param max
    * @return
    */
  def limitValuesOnRange(sparkJdbcProperties: Map[String, String], table: String, colName: String, min: String, max: String): Map[String, String] = {
    spark.read
      .format("jdbc")
      .options(sparkJdbcProperties)
      .option("dbTable", table)
      .load()
      .filter(s"$colName >= $min")
      .filter(s"$colName <= $max")
      .describe(colName)
      .collect()
      .map(row => row.getAs[String]("summary") -> row.getAs[String](colName))
      .toMap
  }

  def limitValues(sparkJdbcProperties: Map[String, String], table: String, colName: String): Map[String, String] = {
    spark.read
      .format("jdbc")
      .options(sparkJdbcProperties)
      .option("dbTable", table)
      .load()
      .describe(colName)
      .collect()
      .map(row => row.getAs[String]("summary") -> row.getAs[String](colName))
      .toMap
  }

  /** @param sparkJdbcProperties  Database connexion configuration. Must have 'url', 'user', 'password', 'driver'
    * @param numPartitions        Number of partitions used to extact data.
    * @param table                Table to extract
    * @return
    */
  def predicatesOverJdbc(sparkJdbcProperties: Map[String, String], numPartitions: Int, table: String): ExtractionPlan = {
    val col = spark.read
      .format("jdbc")
      .options(sparkJdbcProperties)
      .option("dbTable", table)
      .load()
      .schema
      .head

    predicatesOverJdbc(sparkJdbcProperties, numPartitions, table, col.name)
  }

  /** Create the extraction plan
   *  @param sparkJdbcProperties  Database connexion configuration.
    *                             Must have 'url', 'user', 'password', 'driver'. See Apache-Spark Documentation.
    * @param numPartitions        Number of partitions used to extact data.
    * @param table                Table to extract.
    * @param column               Column to be analysed.
    * @return
    */
  def predicatesOverJdbc(sparkJdbcProperties: Map[String, String], numPartitions: Int, table: String, column: String): ExtractionPlan = {
    val col = spark.read
      .format("jdbc")
      .options(sparkJdbcProperties)
      .option("dbTable", table)
      .load()
      .schema(column)
    val columnStats = limitValues(sparkJdbcProperties, table, col.name)
    ExtractionPlan(table, predicatesBuilder(col, numPartitions, columnStats))
  }

}
