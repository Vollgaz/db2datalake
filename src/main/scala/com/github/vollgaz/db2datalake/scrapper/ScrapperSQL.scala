package com.github.vollgaz.db2datalake.scrapper

import com.github.vollgaz.db2datalake.MainConfig
import com.github.vollgaz.db2datalake.SchemaExtension.ExtendedDataFrame
import org.apache.log4j.Logger

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

class ScrapperSQL(config: MainConfig) extends Scrapper(config) {
    val LOGGER: Logger = Logger.getLogger(getClass.getSimpleName)

    def run(): Unit = {

        val tablesConfigs = config.tablesConfig.toParArray
        tablesConfigs.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(config.tablesConcurrence))

        tablesConfigs
            .map(ScrappingPlan.fromString(this.connexionProperties, _, config.tablesPartitions))
            .foreach(plan => {
                spark.sparkContext.setJobDescription(s"dl_${plan.table}")

                LOGGER.info(s"start extraction -- table=${plan.table}")

                spark.read
                    .jdbc(config.dbUrl, plan.table, plan.predicates, connexionProperties)
                    .repartition(8)
                    .cleanWhiteSpaces
                    .write
                    .mode("overwrite")
                    .parquet(config.outputFolderPath + "/" + plan.table)

                LOGGER.info(s"end extraction -- table=${plan.table}, output=${config.outputFolderPath}/${plan.table}")
            })
    }


}
