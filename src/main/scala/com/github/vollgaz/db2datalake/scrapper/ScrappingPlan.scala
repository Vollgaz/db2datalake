package com.github.vollgaz.db2datalake.scrapper

import java.util.Properties

import org.apache.spark.sql.{Row, SparkSession}

import scala.util.matching.Regex

case class ScrappingPlan(table: String, predicates: Array[String])

object ScrappingPlan {
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
  def fromString(connProps: Properties, tableConfig: String, defaultNumPartitions: Int): ScrappingPlan = {
    tableConfig.split("::") match {
      case Array(table, column, config) =>
        config match {
          case regStep(steps)           => predicatesWithProvidedSteps(table, column, steps)
          case regNumPart(numPartition) => predicatesOverJdbc(connProps, numPartition.toInt, table, column)
          case regRange(rangeConf)      => predicatesOverJdbcWithRange(connProps, defaultNumPartitions, table, column, rangeConf)
        }
      case Array(t, c) => predicatesOverJdbc(connProps, defaultNumPartitions, t, c)
      case Array(t)    => predicatesOverJdbc(connProps, defaultNumPartitions, t)
      // TODO : Found proper exception
      case _ => throw new Exception(s"table configuration uninterpretable -- config:$tableConfig")
    }
  }

  /** @param table
    * @param column
    * @param stepsConf
    * @return
    */
  def predicatesWithProvidedSteps(table: String, column: String, stepsConf: String): ScrappingPlan = {
    ScrappingPlan(table, predicatesBuilder.buildPredicates(column, stepsConf.split('~').toArray[Any]))
  }

  /**      *
    * @param connProps
    * @param numPartitions
    * @param table
    * @param column
    * @param rangeConf
    * @return
    */
  def predicatesOverJdbcWithRange(connProps: Properties, numPartitions: Int, table: String, column: String, rangeConf: String): ScrappingPlan = {

    val col = spark.read.jdbc(connProps.getProperty("url"), table, connProps).schema.apply(column)
    rangeConf.split("~") match {
      case Array(min, max, numPartitions) =>
        val limits = limitValuesOnRange(connProps, table, col.name, min, max)
        ScrappingPlan(table, predicatesBuilder(col, numPartitions.toInt, limits))
      case Array(min, max) =>
        val limits = limitValuesOnRange(connProps, table, col.name, min, max)
        ScrappingPlan(table, predicatesBuilder(col, numPartitions, limits))
    }
  }

  def limitValuesOnRange(properties: Properties, table: String, colName: String, min: String, max: String): Map[String, String] = {
    val query =
      s"""(
                SELECT
                    MIN($colName) as min,
                    MAX($colName) as max
                FROM $table
                WHERE $colName >=  $min
                AND $colName <= $max) as myquery
            """
    spark.read
      .jdbc(properties.getProperty("url"), table, properties)
      .filter(s"$colName >= $min")
      .filter(s"$colName <= $max")
      .describe(colName)
      .collect()
      .map(row => row.getAs[String]("summary") -> row.getAs[String](colName))
      .toMap

  }

  // We use position in the row to retrieve the columns MIN and MAX, instead of the column name.
  // It s du to some database using different case system : UPPERCASE name, snake_case name , ....
  def limitValues(properties: Properties, table: String, colName: String): Map[String, String] = {
    val query =
      s"""(SELECT
                    MIN($colName) as min,
                    MAX($colName) as max
                FROM $table) as myquery
            """
    spark.read
      .jdbc(properties.getProperty("url"), table, properties)
      .describe(colName)
      .collect()
      .map(row => row.getAs[String]("summary") -> row.getAs[String](colName))
      .toMap

  }

  /** @param connProps     database connexion configuration. Must have 'url', 'user', 'password', 'driver'
    * @param numPartitions number of partitions used to extact data.
    * @param table         table to extract
    * @return
    */
  def predicatesOverJdbc(connProps: Properties, numPartitions: Int, table: String): ScrappingPlan = {
    val col = spark.read.jdbc(connProps.getProperty("url"), table, connProps).schema.head
    predicatesOverJdbc(connProps, numPartitions, table, col.name)
  }

  /** @param connProps     database connexion configuration. Must have 'url', 'user', 'password', 'driver'
    * @param numPartitions number of partitions used to extact data.
    * @param table         table to extract.
    * @param column        column used to split the data.
    * @return
    */
  def predicatesOverJdbc(connProps: Properties, numPartitions: Int, table: String, column: String): ScrappingPlan = {
    val col = spark.read.jdbc(connProps.getProperty("url"), table, connProps).schema.apply(column)
    val limits = limitValues(connProps, table, col.name)
    ScrappingPlan(table, predicatesBuilder(col, numPartitions, limits))
  }

}
