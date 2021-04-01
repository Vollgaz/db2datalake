package com.github.vollgaz.db2datalake.extraction
import java.util.Properties

import com.github.vollgaz.db2datalake.MainConfig
import com.github.vollgaz.db2datalake.utils.Utils
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import scala.collection.immutable.HashMap

abstract class Extractor(config: MainConfig) {

  def getPassword(): String = {
    (config.dbPassword, config.dbJceksPath) match {
      case (Some(plainPassword), _) => plainPassword
      case (_, Some(keystorePath))  => Utils.readJecksPassword(keystorePath, config.dbUser)
      case _                        => throw (new Exception("Missing credential. A plain password ou JECKS filepath must be provided"))
    }
  }

  val connexionProperties = HashMap(
      "driver"         -> config.dbJdbcDriver,
      "url"            -> config.dbUrl,
      "user"           -> config.dbUser,
      "password"       -> getPassword(),
      "fetchSize"      -> "50000",
      "isolationLevel" -> "READ_UNCOMMITTED"
  )
}
