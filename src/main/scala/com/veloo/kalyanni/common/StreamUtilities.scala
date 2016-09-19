package com.veloo.kalyanni.workloads

object StreamUtilities {

  def getSqlJdbcConnectionString(sqlServerFQDN: String, sqlDatabaseName: String,
                             databaseUsername: String, databasePassword: String): String = {

    val serverName = sqlServerFQDN.split('.')(0)
    val certificateHostname = sqlServerFQDN.replace(serverName, "*")
    val serverPort = "1433"

    val sqlDatabaseConnectionString = f"jdbc:sqlserver://$sqlServerFQDN:$serverPort;database=$sqlDatabaseName;" +
      f"user=$databaseUsername@$serverName;password=$databasePassword;" +
      f"encrypt=true;hostNameInCertificate=$certificateHostname;loginTimeout=30;"

    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

    sqlDatabaseConnectionString
  }
}