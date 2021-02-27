package com.github.vollgaz.db2datalake.schema

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._



object DataframeExtension {
  implicit class Implicits(df: DataFrame) {

    /** The driver jdbc on DB2 return trailing whitespaces.
      * It is mandatory to remove them.
      *
      * @return the same dataframe with corrected data.
      */
    def cleanWhiteSpaces: DataFrame = {
      val cols = df.schema.fields.map(field =>
        field.dataType match {
          case StringType => trim(col(field.name)).as(field.name)
          case _          => col(field.name)
        }
      )
      df.select(cols: _*)
    }

    /** Remove empty strings in data.
      *
      * @return
      */
    def cleanEmptyString: DataFrame = {
      val cols = df.schema.fields.map(field =>
        field.dataType match {
          case StringType => when(col(field.name) === "", None).otherwise(col(field.name)).as(field.name)
          case _          => col(field.name)
        }
      )
      df.select(cols: _*)
    }
  }

}
