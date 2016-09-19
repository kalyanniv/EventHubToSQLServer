package com.veloo.kalyanni.workloads

import java.sql.{Connection, DriverManager}
import org.apache.spark.sql.DataFrame

object DataFrameExtensions {

  implicit def extendedDataFrame(dataFrame: DataFrame) = new ExtendedDataFrame(dataFrame: DataFrame)

  class ExtendedDataFrame(dataFrame: DataFrame) {

    def insertToAzureSql(sqlDatabaseConnectionString: String, sqlTableName: String): Unit = {

      val tableHeader: String = dataFrame.columns.mkString(",")

        dataFrame.foreachPartition { partition =>
          val sqlExecutorConnection: Connection = DriverManager.getConnection(sqlDatabaseConnectionString)

          //Batch size of 1000 is used since Azure SQL database cannot insert more than 1000 rows at the same time.

          partition.grouped(1000).foreach {
            group =>

              val insertString: scala.collection.mutable.StringBuilder = new scala.collection.mutable.StringBuilder()

              group.foreach {
                record => insertString.append("('" + record.mkString(",") + "'),")
              }

              sqlExecutorConnection.createStatement()
                .executeUpdate(f"INSERT INTO [dbo].[$sqlTableName] ($tableHeader) VALUES "
                  + insertString.stripSuffix(","))
          }

          sqlExecutorConnection.close()
        }
    }
  }
}