package com.github.vollgaz.db2datalake.register

import org.apache.log4j.Logger
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructField

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool


/**
 * Open a dataframe and build a create table query to externaly register the file in hive.
 * Replace string type by a varchar equivalent.(Because SAS analytics can't handle properly StringType)
 *
 * @param parallelism Number of columns checked in parallel
 * @param hiveDB      The hive database where the file is registered.
 */
class HiveRegister(parallelism: Int, hiveDB: String) {

    val spark: SparkSession = SparkSession.builder().getOrCreate()
    //val columnDefinitionEncoder: Encoder[ColumnDefinition] = Encoders.product[ColumnDefinition]
    val LOGGER: Logger = Logger.getLogger(getClass.getName)


//    def registerExternalTable(df: DataFrame, tableName: String, hdfsPath: String): Any = {
//        val hive: HiveWarehouseSessionImpl = HiveWarehouseBuilder.session(spark).build()
//        val tableDefinition = analyseDataframe(df, tableName)
//        val queryDrop = buildQueryDrop(hiveDB, tableName)
//        val queryCreate = buildQueryCreateTable(hiveDB, tableName, hdfsPath, tableDefinition)
//        LOGGER.info(queryDrop)
//        LOGGER.info(queryCreate)
//        hive.createDatabase(hiveDB, true)
//        hive.executeUpdate(queryDrop)
//        hive.executeUpdate(queryCreate)
//        hive.close()
//    }

    def buildQueryDrop(hiveDB: String, tableName: String) = s"DROP TABLE IF EXISTS $hiveDB.$tableName"

    def buildQueryCreateTable(hiveDB: String, tableName: String, hdfsPath: String, tableDefinition: Array[ColumnDefinition]): String = {
        s"CREATE EXTERNAL TABLE $hiveDB.$tableName(" + enumerateTableFields(tableDefinition) + s") STORED AS PARQUET LOCATION '$hdfsPath'"
    }


    def enumerateTableFields(colList: Array[ColumnDefinition]): String = {
        val queryStringBuffer = new StringBuilder(s"`${colList.head.colName}` ${colList.head.hiveType}")
        colList.tail.foreach(col => queryStringBuffer.append(s", `${col.colName}` ${col.hiveType}"))
        queryStringBuffer.mkString
    }


    /**
     * Generate a dataframe wich represent the new data structure;
     *
     * @param df
     * @param tableName
     * @return
     */
    def analyseDataframe(df: DataFrame, tableName: String): Array[ColumnDefinition] = {
        val columns = df.schema.fields
            .filterNot(field => (field.dataType.simpleString.contains("struct") || field.dataType.simpleString.contains("array")))
            .toSeq
            .toParArray
        columns.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(parallelism))
        columns.map(column => {
            spark.sparkContext.setJobDescription(s"register_${tableName}_${column.name}")
            if ("string".equals(column.dataType.simpleString)) {
                val data = df.select(column.name).filter(col(column.name).isNotNull).distinct().collect().map(_ (0))
                convertString(data, column)
            }
            else ColumnDefinition(column.name, column.dataType.toString, 0, column.dataType.simpleString)
        }).toArray
    }


    /**
     * Replace spark StringType by Varchar size)
     *
     * @param data
     * @param column structfield object in order to have the name and the data type
     * @return
     */
    def convertString(data: Array[Any], column: StructField): ColumnDefinition = {
        val byteLength = getByteLength(data)
        // 65535 is varchar limit
        // modulo 1 because varchar(0) is meaningless
        if (byteLength < 65535) ColumnDefinition(column.name, column.dataType.toString, byteLength, s"varchar(${byteLength - (byteLength % 1) + 1})")
        else ColumnDefinition(column.name, column.dataType.toString, byteLength, s"string")
    }


    /**
     * Give byte size of the longest string in UTF (care accent use 2 bytes)
     *
     * @param dataArray Array of Any to be su
     * @return longest size
     */
    def getByteLength(dataArray: Array[Any]): Int = {
        if (dataArray.isEmpty) 0
        else dataArray.map(_.toString.getBytes("UTF-8").length).max
    }


}
