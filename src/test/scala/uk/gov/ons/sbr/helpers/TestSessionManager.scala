package uk.gov.ons.sbr.helpers

import org.apache.spark.sql.SparkSession

object TestSessionManager {

  private val appName = "Unit tests Sampling Service app"
  val sparkSession: SparkSession = newSession(appName)

  def newSession(appName: String): SparkSession = {
    println(s"Creating an instance of SparkSession with app name [$appName]")

    val sparkSession = SparkSession
      .builder
      .appName(name = appName)
      .master(master = "local")
      .getOrCreate

    sparkSession
      .sparkContext
      .setLogLevel(logLevel = "ERROR")

    sparkSession
  }
}
