package com.github.vollgaz.db2datalake.extraction
import com.github.vollgaz.db2datalake.MainConfig
import com.github.vollgaz.db2datalake.schema.DataframeExtension.Implicits
import org.apache.log4j.Logger

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import java.util.Properties
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

class ExtractorSQL(config: MainConfig) extends Extractor(config) {
  val LOGGER: Logger = Logger.getLogger(getClass.getSimpleName)

  def run(): Unit = {

    val tablesConfigs = config.tablesConfig.toParArray
    tablesConfigs.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(config.tablesConcurrence))

    tablesConfigs
      .map(ExtractionPlan.fromString(this.connexionProperties, _, config.tablesPartitions))
      .foreach(plan => {
        SparkSession.active.sparkContext.setJobDescription(s"dl_${plan.table}")

        LOGGER.info(s"start extraction -- table=${plan.table}")
        LOGGER.info(s"predicates -- table=${plan.table}, predicates=${plan.predicates.mkString("\n  | ")} ")
        SparkSession.active.read
          .options(connexionProperties)
          .jdbc(config.dbUrl, plan.table, plan.predicates, new Properties())
          .cleanWhiteSpaces
          .write
          .mode(SaveMode.Overwrite)
          .parquet(config.outputFolderPath + "/" + plan.table)

        LOGGER.info(s"end extraction -- table=${plan.table}, output=${config.outputFolderPath}/${plan.table}")
      })
  }

}
