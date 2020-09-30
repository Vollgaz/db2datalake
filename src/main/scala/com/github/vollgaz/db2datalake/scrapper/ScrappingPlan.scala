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


    /**
     *
     * mod1 : table:splitColumn:[min;max;numSteps]
     *
     *
     * mod2 : table:splitColumn:]step1;step2[
     * predicates expected =  splitColumn <=step1; step1 < splitColumn <= step2; step2 < splitColumn
     *
     * @param tableConfig
     * @return
     */
    def fromString(connProps: Properties, tableConfig: String, defaultNumPartitions: Int): ScrappingPlan = {
        tableConfig.split("::") match {
            case Array(table, column, config) => config match {
                case regStep(steps) => predicatesWithProvidedSteps(table, column, steps)
                case regNumPart(numPartition) => predicatesOverJdbc(connProps, numPartition.toInt, table, config)
                case regRange(rangeConf) => predicatesOverJdbcWithRange(connProps, defaultNumPartitions, table, column, rangeConf)
            }
            case Array(t, c) => predicatesOverJdbc(connProps, defaultNumPartitions, t, c)
            case Array(t) => predicatesOverJdbc(connProps, defaultNumPartitions, t)
            case _ => throw new Exception("sdf")
        }
    }

    /**
     *
     * @param table
     * @param column
     * @param stepsConf
     * @return
     */
    def predicatesWithProvidedSteps(table: String, column: String, stepsConf: String): ScrappingPlan = {
        ScrappingPlan(table, predicatesBuilder.buildPredicates(column, stepsConf.split('~').toArray[Any]))
    }


    def predicatesOverJdbcWithRange(connProps: Properties, numPartitions: Int, table: String, column: String,
                                    rangeConf: String): ScrappingPlan = {

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

    def spark: SparkSession = SparkSession.builder().getOrCreate()

    def limitValuesOnRange(properties: Properties, table: String, colName: String, min: String, max: String): Row = {
        val query =
            s"""(SELECT
                    MIN($colName) as min,
                    MAX($colName) as max
                FROM $table) as myquery
                WHERE $colName >=  $min
                AND $colName <= $max
            """
        spark.read.jdbc(properties.getProperty("url"), query, properties).first()
    }

    // We use position in the row to retrieve the columns MIN and MAX, instead of the column name.
    // It s du to some database using different case system : UPPERCASE name, snake_case name , ....
    def limitValues(properties: Properties, table: String, colName: String): Row = {
        val query =
            s"""(SELECT
                    MIN($colName) as min,
                    MAX($colName) as max
                FROM $table) as myquery
            """
        spark.read.jdbc(properties.getProperty("url"), query, properties).first()
    }

    /**
     *
     * @param connProps     connexion properties for accessing the database
     * @param numPartitions default number of partitions
     * @param table         table to extract
     * @return
     */
    def predicatesOverJdbc(connProps: Properties, numPartitions: Int, table: String): ScrappingPlan = {
        val col = spark.read.jdbc(connProps.getProperty("url"), table, connProps).schema.head
        predicatesOverJdbc(connProps, numPartitions, table, col.name)
    }

    /**
     *
     * @param connProps
     * @param numPartitions
     * @param table
     * @param column
     * @return
     */
    def predicatesOverJdbc(connProps: Properties, numPartitions: Int, table: String, column: String): ScrappingPlan = {
        val col = spark.read.jdbc(connProps.getProperty("url"), table, connProps).schema.apply(column)
        val limits = limitValues(connProps, table, col.name)
        ScrappingPlan(table, predicatesBuilder(col, numPartitions, limits))
    }

}
