package uk.gov.ons.sbr.service.validation

import java.nio.file.Path

import org.apache.spark.sql.DataFrame

case class SampleMethodsArguments(
                               unitFrame: DataFrame,
                               stratificationProperties: DataFrame,
                               outputDirectory: Path
                             )
