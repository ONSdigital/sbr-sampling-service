package uk.gov.ons.sbr.service.validation


import org.apache.spark.sql.{DataFrame, SparkSession}
import uk.gov.ons.sbr.service.repository.hive.{HiveFrame, HiveUnitFrameRepository}
import uk.gov.ons.sbr.utils.HadoopPathProcessor.Header
import scala.util.{Success, Try}

case class StratificationMethodArguments(unitFrame: DataFrame,
                                         stratificationProperties: DataFrame,
                                         hiveTable: String,  //this is fully qualified table name with db name prepended
                                         unit:String,
                                         bounds:String
                                        ) extends MethodArguments
object StratificationMethodArguments {


/*  implicit def argsToParams(args:Array[String])(implicit spark: SparkSession) = {
                  Seq[scala.util.Try[Any]] = Seq(
                    HiveUnitFrameRepository.retrieveTableAsDataFrame(HiveFrame(database = args(1), tableName = args(2))),
                    Try{spark.read.option(Header, value = true).csv(args(3))},
                    Success(args(4)),
                    Success(args(5)),
                    Success(args(6))
                  )
  }*/

  def apply(args:Seq[Any]):StratificationMethodArguments = args match {

    case List(
              unitFrame:DataFrame,
              stratificationProperties:DataFrame,
              outputHiveTableName:String,
              unit:String,
              bounds:String
            ) => new StratificationMethodArguments(  unitFrame,
                                                      stratificationProperties,
                                                      outputHiveTableName,
                                                      unit,
                                                      bounds
                                                  )
    case _ => throw new IllegalArgumentException(
      s"cannot create instance of SampleMethodArguments. Invalid arguments: \n" +
        s"stratificationDF: ${args(0).getClass.getCanonicalName()};  \n"+
        s"stratificationProperties: ${args(1).getClass.getCanonicalName()};  \n"+
        s"outputTable: ${args(2).getClass.getCanonicalName()};   \n"+
        s"unit: ${args(3).getClass.getCanonicalName()};   \n" +
        s"bound: ${args(4).getClass.getCanonicalName()};   \n"
    )
  }
}
