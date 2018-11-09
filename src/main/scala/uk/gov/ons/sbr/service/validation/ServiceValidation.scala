package uk.gov.ons.sbr.service.validation

import javax.inject.Singleton
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import uk.gov.ons.sbr.service.repository.UnitFrameRepository
import uk.gov.ons.sbr.service.repository.hive.HiveFrame
import uk.gov.ons.sbr.support.HdfsSupport
import uk.gov.ons.sbr.utils.HadoopPathProcessor.Header
import uk.gov.ons.sbr.globals._
import scala.util.{Try, _}


trait ServiceValidation extends Serializable{


  def parseArgs(args: Array[String],argsToParams:(List[String] ) => Seq[Try[Any]])(implicit spark: SparkSession): MethodArguments = {

    require(args.exists(_.trim().isEmpty),"some arguments missing")

    val sma = {

        val params = argsToParams(args)/*Seq(
                          repository.retrieveTableAsDataFrame(HiveFrame(database = unitFrameDatabaseStr, tableName = unitFrameTableNameStr)),
                          Try{spark.read.option(Header, value = true).csv(stratificationPropertiesStr)},
                          Success(outputHiveDbName),
                          Success(outputHiveTableName),
                          Success(unit),
                          Success(bounds)
                        )*/

        val errors = params.foldRight(""){(el, err) => el match{
          case Failure(e) => s"$err ${e.getMessage} \n"
          case Success(df) => err
        }}

        if(errors.nonEmpty) throw new IllegalArgumentException(s"following arguments errors occurred: $errors")
        else {
          val seq: Seq[Any] = params.map(_.get)
          StratificationMethodArguments(seq)
        }
      }

    sma
  }
}
