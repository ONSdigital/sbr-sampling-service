package uk.gov.ons.sbr.service.repository.hive

object HiveFrame {
  def apply(database: String, tableName: String): String =
    s"$database.$tableName"
}
