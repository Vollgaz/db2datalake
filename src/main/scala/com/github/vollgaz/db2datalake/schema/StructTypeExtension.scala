package com.github.vollgaz.db2datalake.schema

import org.apache.spark.sql.types.StructType

object StructTypeExtension {
  
    implicit class Implicits(struct: StructType) {
        def toDDLNoNull: String = toDDL.replace("NULL", "STRING")

        def toDDL: String = struct.fields.map(_.toDDL).mkString(",")
    }

}
