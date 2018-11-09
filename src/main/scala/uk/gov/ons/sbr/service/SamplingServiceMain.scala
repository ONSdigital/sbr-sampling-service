package uk.gov.ons.sbr.service

import scala.util.Try
import org.apache.spark.sql.{DataFrame, SparkSession}
import uk.gov.ons.registers.methods.{Sample, Stratification}
import uk.gov.ons.sbr.logger.SessionLogger
import uk.gov.ons.sbr.service.repository.hive.{HiveFrame, HiveUnitFrameRepository}
import uk.gov.ons.sbr.service.session.SparkSessionManager
import uk.gov.ons.sbr.service.validation.{MethodArguments, SampleMethodArguments, ServiceValidation, StratificationMethodArguments}
import uk.gov.ons.sbr.support.TrySupport
import uk.gov.ons.sbr.utils.Export


object SamplingServiceMain extends Stratification with ServiceValidation{


  def main(args: Array[String]): Unit = {
    import SparkSessionManager.sparkSession

    SparkSessionManager.withSpark{
      val action = args.head
      SessionLogger.log(msg =s"Initiating $action Service")
      action match{
        case "stratification" => {
          val params: StratificationMethodArguments = parseArgs(args,StratificationMethodArguments.argsToParams
          doStratify(params)
        }
      }

    }}


  def doStratify(args: StratificationMethodArguments)(implicit sparkSession: SparkSession): Unit = {

    SessionLogger.log(msg ="Passed validation. Beginning sample creation process..")

   TrySupport.fold(Try{
       val stratifiedFrameDf = stratify(args.unitFrame, args.stratificationProperties,args.bounds)
       HiveUnitFrameRepository.saveDataFrameToTable(stratifiedFrameDf,args(3),args(4))
   })(onFailure = err =>
      throw new Exception(s"Failed at Stratification method with error [${err.getMessage}]"), onSuccess = identity)
       SessionLogger.log(msg ="Applying stratification method process [Passed].")
  }

  def createSample(args: SampleMethodArguments)(implicit sparkSession: SparkSession): Unit = {

    SessionLogger.log(msg ="Applying stratification method process [Passed].")

    TrySupport.fold(Try(Sample.sample(sparkSession)
      .create(stratifiedFrameDf, args.stratificationProperties)))(onFailure = identity, onSuccess =
      Export(_, args.outputDirectory)
    )
  }
}}
