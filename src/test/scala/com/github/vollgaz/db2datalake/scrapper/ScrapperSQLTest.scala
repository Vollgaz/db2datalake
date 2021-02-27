package com.github.vollgaz.db2datalake.scrapper

import com.github.vollgaz.db2datalake.TitanicDatabaseMock
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.SparkSession

class ScrapperSQLTest extends AnyFlatSpec with BeforeAndAfter {
  val spark: SparkSession = SparkSession.builder().master("local[2]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val mock = new TitanicDatabaseMock()
  before {
    mock.createDBTitanic()
  }

  it should "on table titanic " in {

    
  }

}
