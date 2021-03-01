package com.github.vollgaz.db2datalake.register

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfter

class HiveRegisterTest extends AnyFlatSpec with BeforeAndAfter {

    
    val spark = SparkSession.builder()
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", 1)
        .config("spark.sql.hive.hiveserver2.jdbc.url", "jdbc:hive2://localhost:10000/")
        .config("spark.io.compression.codec", "snappy")
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")    
    val hiveregister = new HiveRegister(1, "test")


    after {
        spark.stop()
    }

    "getByteLength" should "return 10 for 'ééééé'" in {
        val dataToTest = Array[Any]("ééééé")
        val result = hiveregister.getByteLength(dataToTest)
        assert(result == 10)
    }
    it should "find alone the longest value" in {
        val dataToTest = Array[Any]("ééééé", "osdfhdfovodmf", "pdfjpdjv", "$ù^ù$ù", "§$^*")
        val result = hiveregister.getByteLength(dataToTest)
        assert(result == 13)
    }

    it should "return 8 for 'abcdefgh'" in {
        val dataToTest = Array[Any]("abcdefgh")
        val result = hiveregister.getByteLength(dataToTest)
        assert(result == 8)
    }
    it should "return 0 for empty string " in {
        val dataToTest = Array[Any]("")
        val result = hiveregister.getByteLength(dataToTest)
        assert(result == 0)
    }
    it should "return 0 for empty data " in {
        val dataToTest = Array[Any]()
        val result = hiveregister.getByteLength(dataToTest)
        assert(result == 0)
    }
    "convertString" should "return  varchar(10)" in {
        val dataToTest = Array[Any]("dsfonosdf")
        val column = StructField("colTest", StringType)
        val expectedResult = ColumnDefinition("colTest", "StringType", 9, "varchar(10)")
        assert(hiveregister.convertString(dataToTest, column) == expectedResult)
    }
    it should "return varchar(1) for empty column of stringType" in {
        val dataToTest = Array[Any]()
        val column = StructField("colTest", StringType)
        val expectedResult = ColumnDefinition("colTest", "StringType", 0, "varchar(1)")
        assert(hiveregister.convertString(dataToTest, column) == expectedResult)
    }

    it should "return varchar(1) for column filled with empty string" in {
        val dataToTest = Array[Any]("")
        val column = StructField("colTest", StringType)
        val expectedResult = ColumnDefinition("colTest", "StringType", 0, "varchar(1)")
        assert(hiveregister.convertString(dataToTest, column) == expectedResult)
    }

    it should "not convert String longer than 65535" in {
        val hugestring: String = (Array.fill(65535) {
            "X"
        }).mkString
        val dataToTest = Array[Any](hugestring)
        val column = StructField("colTest", StringType)
        val expectedResult = ColumnDefinition("colTest", "StringType", 65535, "string")
        assert(hiveregister.convertString(dataToTest, column) == expectedResult)
    }

    "analyseDataframe" should "convert only StringType" in {
        import spark.implicits._
        val dataToTest = List(
            ("dsfsdf", 12, 1233.3),
            ("abd", 978, 0.6666666))
            .toDF("col1", "col2", "col3")

        val result = hiveregister.analyseDataframe(dataToTest, "testTable")
        assert(result(0) == ColumnDefinition("col1", "StringType", 6, "varchar(7)"))
        assert(result(1) == ColumnDefinition("col2", "IntegerType", 0, "int"))
        assert(result(2) == ColumnDefinition("col3", "DoubleType", 0, "double"))
    }
    it should "ignore complex type" in {
        import spark.implicits._
        val dataToTest = List(
            ("dsfsdf", 12, ("ted", 1), Array(2323, 23244, 2546), "sdfsdf"),
            ("abd", 978, ("reter", 176), Array(3435, 35355, 3535), "CINDVCIKBSV"))
            .toDF("col1", "col2", "col3", "col4", "col5")

        val result = hiveregister.analyseDataframe(dataToTest, "testTable")
        assert(result.length == 3)
        assert(result(0) == ColumnDefinition("col1", "StringType", 6, "varchar(7)"))
        assert(result(1) == ColumnDefinition("col2", "IntegerType", 0, "int"))
        assert(result(2) == ColumnDefinition("col5", "StringType", 11, "varchar(12)"))

    }

    "buildQueryDrop" should "work" in {
        val result = hiveregister.buildQueryDrop("dbtest", "coltest")
        val expectedResult = s"DROP TABLE IF EXISTS dbtest.coltest";
        assert(result == expectedResult)
    }

    "buildQueryCreateTable" should "work" in {
        val columnDefinition: Array[ColumnDefinition] = Array(
            ColumnDefinition("col1", "StringType", 6, "varchar(7)"),
            ColumnDefinition("col2", "IntegerType", 0, "int"))
        val result = hiveregister.buildQueryCreateTable("dbtest", "coltest", "/tmp/testfile", columnDefinition)
        val expectedResult = s"CREATE EXTERNAL TABLE dbtest.coltest(`col1` varchar(7), `col2` int) STORED AS PARQUET LOCATION '/tmp/testfile'";
        assert(result == expectedResult)
    }
}
