package com.github.vollgaz.db2datalake

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}
import scala.collection.immutable.HashMap
import java.nio.file.Files
import java.nio.file.Paths

trait TitanicDatabaseMock {

  val dbUrl = "jdbc:sqlite:target/sqlite.db"
  val table = "titanic"
  val jdbcDriver = "org.sqlite.JDBC"

  val defaultProperties = HashMap(
      "url"     -> dbUrl,
      "driver"  -> jdbcDriver,
      "dbTable" -> table
  )

  val defaultNumPartitions = 4

  def createDBTitanic(): Unit = {

    Class.forName(jdbcDriver)
    SparkSession.active.read
      .option("sep", ",")
      .option("header", true)
      .option("inferSchema", true)
      .csv("src/test/resources/titanic.csv")
      .coalesce(1) // Needed as sqlite allow only one connexion in write
      .write
      .format("jdbc")
      .mode(SaveMode.Overwrite)
      .options(defaultProperties)
      .save()
  }

  def cleanDBTitanic(): Unit = {
    Files.delete(Paths.get("target/sqlite.db"))
  }

}
