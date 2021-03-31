package com.github.vollgaz.db2datalake.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.alias.CredentialProviderFactory
import org.apache.log4j.Logger

object Utils {
  val LOGGER: Logger = Logger.getLogger(getClass.getSimpleName)

  /** Retrieve a password stored in a jceks file on hdfs.
    * @param jceks_path Must respect the format "jceks://hdfs/_PATH_TO_FILE_ON_HDFS_"
    * @param dbuser The user for connecting the database or the alias used to registered the password
    *               By convention i recommend to use the user instead of an alias.
    * @return password associated to user/alias
    */
  def readJecksPassword(jceks_path: String, dbuser: String): String = {
    LOGGER.info(s"Extracting password for $dbuser in $jceks_path")
    val conf = new Configuration()
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, jceks_path)
    conf.getPassword(dbuser).mkString
  }

  def buildMongoString(user: String, passw: String, nodes: String, collection: String, connexion_args: String = "") =
    s"mongodb://$user:$passw@$nodes/$user.$collection?authSource=admin;$connexion_args"

}
