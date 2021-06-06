package com.github.vollgaz.db2datalake

import org.scalatest.Suite
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.SparkContext
import java.util.Date
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.internal.config.SparkConfigProvider
import java.util.logging.Logger
import java.util.logging.Filter

trait SharedSparkSession extends AnyFlatSpec with BeforeAndAfterAll with TitanicDatabaseMock {
  val master = "local[2]"
  val className = getClass().getSimpleName()
  private lazy val _spark = SparkSession.builder().appName(className).master("local[2]").getOrCreate()
  _spark.sparkContext.setLogLevel("ERROR")

  override def beforeAll() {
    super.beforeAll()
    createDBTitanic()
  }

  override def afterAll() {
    super.afterAll()
    this.cleanDBTitanic()
  }

}
