package com.github.vollgaz.db2datalake.extraction

import com.github.vollgaz.db2datalake.TitanicDatabaseMock
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfterAll
import com.github.vollgaz.db2datalake.SharedSparkSession

class ExtractionPlanTest extends SharedSparkSession {

  /** Provide only the table
    * The column will be found and the number of partitions is the default value.
    */
  "fromString" should "decode 'titanic'" in {
    val input = "titanic"
    val expected = ExtractionPlan(
        "titanic",
        predicates = Array[String](
            "PassengerId < 223",
            "PassengerId >= 223 AND PassengerId < 445",
            "PassengerId >= 445 AND PassengerId < 667",
            "PassengerId >= 667"
        )
    )

    val actual = ExtractionPlan.fromString(defaultProperties, input, defaultNumPartitions)
    assert(actual.table == expected.table)
    assert(actual.predicates sameElements expected.predicates)
  }

  /** Provide table and column
    * The number of partition is the default value passed by execution args (here 4)
    */
  it should "decode 'titanic::PassengerId'" in {
    val input = "titanic::PassengerId"
    val expected = ExtractionPlan(
        "titanic",
        predicates = Array[String](
            "PassengerId < 223",
            "PassengerId >= 223 AND PassengerId < 445",
            "PassengerId >= 445 AND PassengerId < 667",
            "PassengerId >= 667"
        )
    )

    val actual = ExtractionPlan.fromString(defaultProperties, input, defaultNumPartitions)
    assert(actual.table == expected.table)
    assert(actual.predicates sameElements expected.predicates)
  }

  /** Provide table, column, min and max values
    * The number of partition is the default value passed by execution args (here 4)
    */
  it should "decode 'titanic::PassengerId::[20~700]'" in {
    val input = "titanic::PassengerId::[20~700]"
    val expected = ExtractionPlan(
        "titanic",
        predicates = Array[String](
            "PassengerId < 190",
            "PassengerId >= 190 AND PassengerId < 360",
            "PassengerId >= 360 AND PassengerId < 530",
            "PassengerId >= 530"
        )
    )

    val actual = ExtractionPlan.fromString(defaultProperties, input, defaultNumPartitions)
    assert(actual.table == expected.table)
    assert(actual.predicates sameElements expected.predicates)
  }

  /** Provide table, column, min / max values and the number of partitions
    */
  it should "decode 'titanic::PassengerId::[20~700~3]'" in {
    val input = "titanic::PassengerId::[20~700~3]"
    val expected = ExtractionPlan(
        "titanic",
        predicates = Array[String](
            "PassengerId < 246",
            "PassengerId >= 246 AND PassengerId < 472",
            "PassengerId >= 472"
        )
    )

    val actual = ExtractionPlan.fromString(defaultProperties, input, defaultNumPartitions)
    assert(actual.table == expected.table)
    assert(actual.predicates sameElements expected.predicates)
  }

  /** Provide table anc column
    * The number of partitions is overriden
    */
  it should "decode 'titanic::PassengerId::{2}'" in {
    val input = "titanic::PassengerId::{2}"
    val expected = ExtractionPlan(
        "titanic",
        predicates = Array[String](
            "PassengerId < 446",
            "PassengerId >= 446"
        )
    )

    val actual = ExtractionPlan.fromString(defaultProperties, input, defaultNumPartitions)
    assert(actual.table == expected.table)
    assert(actual.predicates sameElements expected.predicates)
  }

  /** Provide steps to build the ranges on int
    * Doesn't require a JDBC connexion
    */
  it should "decode 'MYTABLE::MYCOL::]10~20['" in {
    val input = "MYTABLE::MYCOL::]10~20["
    val expected = ExtractionPlan(
        "MYTABLE",
        Array[String](
            "MYCOL < 10",
            "MYCOL >= 10 AND MYCOL < 20",
            "MYCOL >= 20"
        )
    )
    val actual = ExtractionPlan.fromString(defaultProperties, input, defaultNumPartitions)

    assert(actual.table == expected.table)
    assert(actual.predicates sameElements expected.predicates)
  }

  /** Provide steps to build the ranges on timestamp
    * Doesn't require a JDBC connexion
    */
  it should "decode 'MYTABLE::MYCOL::]'2000-01-01 00:00:00'~'2020-01-01 12:12:12'['" in {
    val input = "MYTABLE::MYCOL::]'2000-01-01 00:00:00'~'2020-01-01 12:12:12'["
    val expected = ExtractionPlan(
        "MYTABLE",
        Array[String](
            "MYCOL < '2000-01-01 00:00:00'",
            "MYCOL >= '2000-01-01 00:00:00' AND MYCOL < '2020-01-01 12:12:12'",
            "MYCOL >= '2020-01-01 12:12:12'"
        )
    )
    val actual = ExtractionPlan.fromString(defaultProperties, input, defaultNumPartitions)

    assert(actual.table == expected.table)
    assert(actual.predicates sameElements expected.predicates)
  }

  /** Provide steps to build the ranges on timestamp
    * Doesn't require a JDBC connexion
    */
  it should "handle split on string column" in {
    val input = "titanic::Name"
    val expected = ExtractionPlan("titanic", Array[String]("1=1"))
    val actual = ExtractionPlan.fromString(defaultProperties, input, defaultNumPartitions)

    assert(actual.table == expected.table)
    assert(actual.predicates sameElements expected.predicates)
  }
}
