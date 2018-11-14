package uk.gov.ons.sbr.service.validation

import javax.inject.Singleton
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import uk.gov.ons.sbr.service.repository.UnitFrameRepository
import uk.gov.ons.sbr.service.repository.hive.{HiveFrame, HiveUnitFrameRepository}
import uk.gov.ons.sbr.support.HdfsSupport
import uk.gov.ons.sbr.utils.HadoopPathProcessor.Header
import uk.gov.ons.sbr.globals._

import scala.tools.nsc.typechecker.Macros
import scala.util.{Try, _}


@Singleton
class ServiceValidation(repository: UnitFrameRepository) {


  def parseSamplingArgs(args: List[String])(implicit spark: SparkSession): SampleMethodArguments = {
    import spark.implicits._


    val sma = args.filterNot(_.trim().isEmpty) match{

      case List(_,unitFrameDatabaseStr,unitFrameTableNameStr,stratificationPropertiesStr,outputDBName, outputTableName) => {
        val params:Seq[scala.util.Try[Any]] = Seq(
          repository.retrieveTableAsDataFrame(HiveFrame(database = unitFrameDatabaseStr, tableName = unitFrameTableNameStr)),
          Try{spark.read.option(Header, value = true).csv(stratificationPropertiesStr)},
          Success(HiveFrame(outputDBName, outputTableName))
        )

        val errors = params.foldRight(""){(el, err) => el match{
          case Failure(e) => s"$err ${e.getMessage} \n"
          case Success(df) => err
        }}

        if(!errors.isEmpty) throw new IllegalArgumentException(s"following arguments errors occurred: $errors")
        else SampleMethodArguments(params.map(_.get))


      }
      case _ => throw new IllegalArgumentException(s"wrong number of arguments: expected 6, actual ${args.length}")
    }

    sma
  }

  /*
     *    [0] "stratification"
    *     [1] input data hive DB name
    *     [2] input data hive table name
    *     [3] path to properties csv
    *     [4] output hive DB name,
    *     [5] output hive table name,
    *     [6] unit name,
    *     [7] bounds
  * */
    def parseStratificationArgs(args: List[String])(implicit spark: SparkSession): StratificationMethodArguments = {

    val sma = args.filterNot(_.trim().isEmpty) match{

      case List(_,unitFrameDatabaseStr,unitFrameTableNameStr,stratificationPropertiesStr,outputDBName, outputTableName,unit,bounds) => {

        val params:Seq[scala.util.Try[Any]] = Seq(
          repository.retrieveTableAsDataFrame(HiveFrame(database = unitFrameDatabaseStr, tableName = unitFrameTableNameStr)),
          Try{spark.read.option(Header, value = true).csv(stratificationPropertiesStr)},
          Success(HiveFrame(outputDBName, outputTableName)),
          Success(unit),
          Success(bounds)
        )

        val errors = params.foldRight(""){(el, err) => el match{
          case Failure(e) => s"$err ${e.getMessage} \n"
          case Success(df) => err
        }}

        if(!errors.isEmpty) throw new IllegalArgumentException(s"following arguments errors occurred: $errors")
        else StratificationMethodArguments(params.map(_.get))

      }
      case _ => throw new IllegalArgumentException(s"wrong number of arguments: expected 8, actual ${args.length}")
    }

    sma
  }



}