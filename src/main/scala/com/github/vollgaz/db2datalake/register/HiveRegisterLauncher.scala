//package com.github.vollgaz.db2datalake.register
//
//import org.apache.log4j.Logger
//import org.apache.spark.sql.SparkSession
//
//import scala.collection.parallel.ForkJoinTaskSupport
//import scala.concurrent.forkjoin.ForkJoinPool
//
//object HiveRegisterLauncher {
//
//    val spark: SparkSession = SparkSession.builder().getOrCreate()
//    val LOGGER: Logger = Logger.getLogger(getClass.getName)
//
//    def main(args: Array[String]): Unit = {
//
//        val input_path = args(0)
//        val tables_list = args(1)
//        val hive_db = args(2)
//        val parallel_table = args(3).toInt
//        val parallel_col = args(4).toInt
//        val tables = tables_list.split(",").toSeq.toParArray
//        val register = new HiveRegister(parallel_table, hive_db)
//
//        tables.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(parallel_col))
//
//        LOGGER.info("input_path : " + input_path)
//        LOGGER.info("tables_list : " + tables_list)
//        LOGGER.info("hive_db : " + hive_db)
//        LOGGER.info("parallel_table : " + parallel_table)
//        LOGGER.info("parallel_col : " + parallel_col)
//
//
//        tables.foreach((table: String) => {
//            val tablePath = input_path + "/" + table
//            val df = spark.read.parquet(tablePath)
//            register.registerExternalTable(df, table, tablePath)
//        })
//    }
//}