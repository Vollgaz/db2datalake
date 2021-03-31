package com.github.vollgaz.db2datalake

import scopt.{OParser, OParserBuilder}

class MainArgsParser(args: Array[String]) {
    val builder: OParserBuilder[MainConfig] = OParser.builder[MainConfig]
    val parser: OParser[Unit, MainConfig] = {
        import builder._
        OParser.sequence(
            programName(this.getClass.getPackage.getImplementationTitle),
            head("version", this.getClass.getPackage.getImplementationVersion),
            help("help").text("Print help"),
            cmd("scrapper")
                .action((_, config) => config.copy(mode = "scrapper"))
                .text("Extract databases to parquet file.\n")
                .children(
                    opt[String]('U', "db-url")
                        .required()
                        .action((value, config) => config.copy(dbUrl = value))
                        .text("Database url in jdbc format."),
                    opt[String]('d', "jdbc-driver")
                        .required()
                        .action((value, config) => config.copy(dbJdbcDriver = value))
                        .text("Driver name required for connecting the database"),
                    opt[String]('u', "db-user")
                        .required()
                        .action((value, config) => config.copy(dbUser = value))
                        .text("Username to authenticate to the database"),
                    opt[String]('j', "jceks-file")
                        .required()
                        .action((value, config) => config.copy(dbJceksPath = Some(value)))
                        .text("File containing the password for the user"),
                    opt[String]('o', "output")
                        .required()
                        .action((value, config) => config.copy(outputFolderPath = value))
                        .text("Destination folder for the data"),
                    opt[Int]("tables-concurrence")
                        .action((value, config) => config.copy(tablesConcurrence = value))
                        .text("Number of tables to be extracted at the same time. Default is 4"),
                    opt[Int]("table-partitions")
                        .action((value, config) => config.copy(tablesPartitions = value))
                        .text("Number of partitions used to extract the table. Default is 16"),
                    opt[Seq[String]]("tables")
                        .valueName("table1,table2,table3...")
                        .action((value, config) => config.copy(tablesConfig = value))
                        .text("Tables to extract")
                ),
            cmd("register")
                .action((_, config) => config.copy(mode = "register"))
                .text("Register parquet to hive.\n")
                .children(
                    opt[String]('f', "input-folder")
                        .required()
                        .action((value, config) => config.copy(dbUrl = value))
                        .text("Database url in jdbc format.")

                ),
            opt[Int]("tables-concurrence")
                .action((value, config) => config.copy(tablesConcurrence = value))
                .text("Number of tables to be analysed at the same time. Default is 4"),

            opt[Seq[String]]("tables")
                .valueName("table1,table2,table3...")
                .action((value, config) => config.copy(tablesConfig = value))
                .text("Tables to extract"),
            opt[Seq[String]]("hive-db")
                .valueName("table1,table2,table3...")
                .action((value, config) => config.copy(tablesConfig = value))
                .text("Tables to extract")
        )
    }

    def getConfig: Option[MainConfig] = {
        OParser.parse(parser, args, MainConfig()) match {
            case Some(config) => Some(config)
            case _ => None
        }
    }
}