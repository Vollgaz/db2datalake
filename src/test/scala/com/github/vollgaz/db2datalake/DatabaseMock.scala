package com.github.vollgaz.db2datalake

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

class TitanicDatabaseMock {

  val dbUrl = "jdbc:sqlite:target/sqlite.db"

  val defaultProperties: Properties = {
    val props = new Properties()
    props.setProperty("url", dbUrl)
    props.setProperty("driver", "org.sqlite.JDBC")
    props
  }

  val table = "titanic"

  val defaultNumPartitions = 4

  def createDBTitanic(): Unit = {

    Class.forName("org.sqlite.JDBC")
    SparkSession.active.read
      .option("sep", ",")
      .option("header", true)
      .option("inferSchema", true)
      .csv("src/test/resources/titanic.csv")
      .coalesce(1) // Needed as sqlite allow only one connexion in write
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(url = dbUrl, table = "titanic", connectionProperties = new Properties())

  }
}
