package uk.gov.ons.sbr.service.validation

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import uk.gov.ons.sbr.service.repository.hive.{HiveFrame, HiveUnitFrameRepository}
import uk.gov.ons.sbr.utils.HadoopPathProcessor.Header

import scala.util.{Success, Try}


case class SampleMethodArguments(
                                   stratificationDF: DataFrame,
                                   samplingProperties: DataFrame,
                                   outputDbName: String,
                                   outputTableName: String
                                 ) extends MethodArguments
object SampleMethodArguments{

/*  implicit def argsToParams(args:Array[String])(implicit spark: SparkSession) = {
    Seq[scala.util.Try[Any]] = Seq(
      HiveUnitFrameRepository.retrieveTableAsDataFrame(HiveFrame(database = args(1), tableName = args(2))),
      Try{spark.read.option(Header, value = true).csv(args(3))},
      Success(args(4)),
      Success(args(5))
    )
  }*/

  def apply(args:Seq[Any]):SampleMethodArguments = args match {

    case List(
              stratificationDF:DataFrame,
              samplingProperties:DataFrame,
              outputDbName: String,
              outputTableName: String
              ) => new SampleMethodArguments( stratificationDF,
                                              samplingProperties,
                                              outputDbName,
                                              outputTableName
                                             )
    case _ => throw new IllegalArgumentException(
                                                  s"cannot create instance of SampleMethodArguments. Invalid arguments: \n" +
                                                  s"stratificationDF: ${args(0).getClass.getCanonicalName()};  \n"+
                                                  s"stratificationProperties: ${args(1).getClass.getCanonicalName()};  \n"+
                                                  s"stratificationFilePath: ${args(2).getClass.getCanonicalName()};   \n"+
                                                  s"outputDirectoryPath: ${args(3).getClass.getCanonicalName()};   \n"
                                                 )
     }
}
