package com.github.vollgaz.db2datalake.register

case class ColumnDefinition(colName: String,
                            colType: String,
                            colByteLength: Int,
                            hiveType: String)