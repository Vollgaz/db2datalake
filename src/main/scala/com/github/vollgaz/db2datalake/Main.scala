package com.github.vollgaz.db2datalake

import com.github.vollgaz.db2datalake.extraction.ExtractorSQL
import org.apache.spark.sql.SparkSession

object Main extends App {
    val spark = SparkSession.getActiveSession match {
        case e: Option[SparkSession] => e.get
        case _ => SparkSession.builder().master("local[*]").getOrCreate()
    }
    new MainArgsParser(args).getConfig match {

        case e: Option[MainConfig] => e.get.mode match {
            case "scrapper" => new ExtractorSQL(e.get)
            case "register" => throw new NotImplementedError()
        }
    }





}
