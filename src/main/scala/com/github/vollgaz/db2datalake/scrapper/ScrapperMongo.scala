package com.github.vollgaz.db2datalake.scrapper

import com.github.vollgaz.db2datalake.SchemaExtension.ExtendedStructType
import com.github.vollgaz.db2datalake.utils.Utils
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

object ScrapperMongo {

    val spark: SparkSession = SparkSession.builder().getOrCreate()
    val LOGGER: Logger = Logger.getLogger(getClass.getSimpleName)

    def main(args: Array[String]): Unit = {

        val mongo_servers = args(0)
        val collections_list = args(1)
        val dbuser = args(2)
        val jckes_path = args(3)
        val output_path = args(4)
        val parallelism_collections = args(5).toInt

        val passw = Utils.readJecksPassword(jckes_path, dbuser)
        val collections = collections_list.split(",").toSeq.toParArray
        collections.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(parallelism_collections))

        LOGGER.info("mongo_servers : " + mongo_servers)
        LOGGER.info("collections_list : " + collections_list)
        LOGGER.info("dbuser : " + dbuser)
        LOGGER.info("jckes_path : " + jckes_path)
        LOGGER.info("output_path : " + output_path)

        collections.foreach(collection => {
            spark.sparkContext.setJobDescription(s"dl_$collection")
            LOGGER.info("start extraction " + collection)
            val connexionString = Utils.buildMongoString(dbuser, passw, mongo_servers, collection)

            val df = spark.read.format("com.mongodb.spark.sql.DefaultSource")
                .option("sampleSize", 100000)
                .option("uri", connexionString)

            val cleanedSchema = df.load().schema.toDDLNoNull

            df.schema(cleanedSchema)
                .load()
                .repartition(8)
                .write
                .mode("overwrite")
                .parquet(output_path + "/" + collection)
            LOGGER.info("end extraction " + collection + " to " + output_path + "/" + collection)
        })
    }
}
