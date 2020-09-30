package com.github.vollgaz.db2datalake

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, trim, when}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SchemaExtension {

    /**
     *
     * @param df
     */
    implicit class ExtendedDataFrame(df: DataFrame) {
        /**
         * The driver jdbc on DB2 return trailing whitespaces.
         * It is mandatory to remove them.
         *
         * @return the same dataframe with corrected data.
         */
        def cleanWhiteSpaces: DataFrame = {
            val cols = df.schema.fields.map(field => field.dataType match {
                case StringType => trim(col(field.name)).as(field.name)
                case _ => col(field.name)
            })
            df.select(cols: _*)
        }

        /**
         * Remove empty strings in data.
         *
         * @return
         */
        def cleanEmptyString: DataFrame = {
            val cols = df.schema.fields.map(field => field.dataType match {
                case StringType => when(col(field.name) === "", None).otherwise(col(field.name)).as(field.name)
                case _ => col(field.name)
            })
            df.select(cols: _*)
        }
    }

    /**
     *
     * @param struct
     */
    implicit class ExtendedStructType(struct: StructType) {
        def toDDLNoNull: String = toDDL.replace("NULL", "STRING")

        def toDDL: String = struct.fields.map(_.toDDL).mkString(",")
    }

    /**
     *
     * @param field
     */
    implicit class ExtendedStructField(field: StructField) {
        /**
         * Returns a string containing a schema in DDL format. For example, the following value:
         * `StructField("eventId", IntegerType)` will be converted to `eventId` INT.
         *
         * @since 2.4.0
         */
        def toDDL: String = {
            val comment = getComment()
                .map(escapeSingleQuotedString)
                .map(" COMMENT '" + _ + "'")

            s"${quoteIdentifier(field.name)} ${field.dataType.sql}${comment.getOrElse("")}"
        }

        /**
         * Return the comment of this StructField.
         */
        def getComment(): Option[String] = {
            if (field.metadata.contains("comment")) Option(field.metadata.getString("comment")) else None
        }
    }

    def quoteIdentifier(name: String): String = {
        // Escapes back-ticks within the identifier name with double-back-ticks, and then quote the
        // identifier with back-ticks.
        "`" + name.replace("`", "``") + "`"
    }

    def escapeSingleQuotedString(str: String): String = {
        val builder = StringBuilder.newBuilder

        str.foreach {
            case '\'' => builder ++= s"\\\'"
            case ch => builder += ch
        }

        builder.toString()
    }
}
