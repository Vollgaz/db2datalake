package com.github.vollgaz.db2datalake.scrapper

import java.util.Properties

import com.github.vollgaz.db2datalake.MainConfig
import com.github.vollgaz.db2datalake.utils.Utils
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

abstract class Scrapper(config: MainConfig) {

    val spark: SparkSession = SparkSession.builder().getOrCreate()

    val connexionProperties = new Properties()
    connexionProperties.put("driver", config.dbJdbcDriver)
    connexionProperties.put("url", config.dbUrl)
    connexionProperties.put("user", config.dbUser)
    connexionProperties.put("password", Utils.readJecksPassword(config.dbJceksPath, config.dbUser))
    connexionProperties.put("fetchSize", "50000")
    connexionProperties.put("isolationLevel", "READ_UNCOMMITTED")


}
