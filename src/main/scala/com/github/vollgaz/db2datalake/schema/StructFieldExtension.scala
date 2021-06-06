package com.github.vollgaz.db2datalake.schema

import org.apache.spark.sql.types.StructField

object StructFieldExtension {
  implicit class Implicits(val field: StructField) extends AnyVal {
        /**
         * Returns a string containing a schema in DDL format. For example, the following value:
         * `StructField("eventId", IntegerType)` will be converted to `eventId` INT.
         *
         * @backport 2.4.0
         */
        def toDDL: String = {
            val comment = getComment()
                .map(escapeSingleQuotedString)
                .map(" COMMENT '" + _ + "'")

            s"${quoteIdentifier(field.name)} ${field.dataType.sql}${comment.getOrElse("")}"
        }

        /**
         * Return the comment of this StructField.
         * @backport 2.4.0
         */
        def getComment(): Option[String] = {
            if (field.metadata.contains("comment")) Option(field.metadata.getString("comment")) else None
        }
    }

    /**
      * @backport 2.4.0
      *
      * @param name
      * @return
      */
    def quoteIdentifier(name: String): String = {
        // Escapes back-ticks within the identifier name with double-back-ticks, and then quote the
        // identifier with back-ticks.
        "`" + name.replace("`", "``") + "`"
    }

    /**
      * @backport 2.4.0
      *
      * @param str
      * @return
      */
    def escapeSingleQuotedString(str: String): String = {
        val builder = StringBuilder.newBuilder

        str.foreach {
            case '\'' => builder ++= s"\\\'"
            case ch => builder += ch
        }

        builder.toString()
    }
}
