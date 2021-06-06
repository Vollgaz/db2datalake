package com.github.vollgaz.db2datalake.extraction

import com.github.vollgaz.db2datalake.TitanicDatabaseMock
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.SparkSession
import com.github.vollgaz.db2datalake.MainConfig
import org.scalatest.BeforeAndAfterAll
import com.github.vollgaz.db2datalake.SharedSparkSession

class ExtractorSQLTest extends SharedSparkSession {
  
  "db2datalake" should "produce a parquet file from Sqlite table titanic" in {
    val configTest = MainConfig(
        mode = "scrapper",
        dbUrl = dbUrl,
        dbJdbcDriver = jdbcDriver,
        dbUser = "",
        dbJceksPath = Some(""),
        dbPassword = Some(""),
        outputFolderPath = "target/testout/",
        tablesConfig = Seq("titanic::PassengerId::{5}"),
        tablesConcurrence = 1,
        tablesPartitions = 3
    )
    val expectedOutputPath = "target/testout/titanic"
    val actual = new ExtractorSQL(configTest)
    actual.run()

    val df = SparkSession.builder.getOrCreate.read.parquet(expectedOutputPath)
    assert(df.count == 891)
  }

}
