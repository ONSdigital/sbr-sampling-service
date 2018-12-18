package uk.gov.ons.sbr.logger

// NOTE: Patch until actual logging is set up
object SessionLogger {
  def log(level: String = "info",  msg: String): Unit =
    println(s"[${level.capitalize}] $msg")
}
