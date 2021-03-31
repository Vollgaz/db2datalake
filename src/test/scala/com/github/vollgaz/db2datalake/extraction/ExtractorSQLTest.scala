package com.github.vollgaz.db2datalake.extraction

import com.github.vollgaz.db2datalake.TitanicDatabaseMock
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.SparkSession
import com.github.vollgaz.db2datalake.MainConfig
import org.scalatest.BeforeAndAfterAll

class ExtractorSQLTest extends AnyFlatSpec with BeforeAndAfterAll {

  lazy val mock = new TitanicDatabaseMock()
  lazy val spark = SparkSession.active

  override def beforeAll() {
    SparkSession.builder().master("local[2]").getOrCreate()
    SparkSession.active.sparkContext.setLogLevel("ERROR")
    mock.createDBTitanic()
  }

  override def afterAll() {
    spark.stop()
  }

  "db2datalake" should "produce a parquet file from Sqlite table titanic" taggedAs (org.scalatest.tagobjects.Disk) in {
    import spark.implicits._
    val configTest = MainConfig(
        mode = "scrapper",
        dbUrl = mock.dbUrl,
        dbJdbcDriver = mock.jdbcDriver,
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

    val df = spark.read.parquet(expectedOutputPath)
    assert(df.count == 891)
  }

}
