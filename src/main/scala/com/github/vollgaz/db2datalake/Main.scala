package com.github.vollgaz.db2datalake

import com.github.vollgaz.db2datalake.scrapper.ScrapperSQL
import org.apache.spark.sql.SparkSession

object Main extends App {
    val spark = SparkSession.getActiveSession match {
        case e: Option[SparkSession] => e.get
        case _ => SparkSession.builder().master("local[*]").getOrCreate()
    }
    new MainArgsParser(args).getConfig match {

        case e: Option[MainConfig] => e.get.mode match {
            case "scrapper" => new ScrapperSQL(e.get)
            case "register" => throw new NotImplementedError()
        }
    }





}
