package com.github.vollgaz.db2datalake


case class MainConfig(mode: String = "help",
                      dbUrl: String = "http://localhost:5347",
                      dbJdbcDriver: String = "",
                      dbUser: String = "",
                      dbPassword :Option[String] = None,
                      dbJceksPath: Option[String] = None,
                      outputFolderPath: String = "",
                      tablesConfig: Seq[String] = Seq.empty[String],
                      tablesConcurrence: Int = 4,
                      tablesPartitions: Int = 16,
                      hiveDb: String = "default")


