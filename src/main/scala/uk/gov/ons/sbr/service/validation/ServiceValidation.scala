package uk.gov.ons.sbr.service.validation

import javax.inject.Singleton
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import uk.gov.ons.sbr.service.repository.UnitFrameRepository
import uk.gov.ons.sbr.service.repository.hive.HiveFrame
import uk.gov.ons.sbr.support.HdfsSupport
import uk.gov.ons.sbr.utils.HadoopPathProcessor.Header

import scala.util.{Try, _}

@Singleton
class ServiceValidation(repository: UnitFrameRepository) {


  def parseArgs(args: List[String])(implicit spark: SparkSession): SampleMethodsArguments = {
    import spark.implicits._


    val sma = args.filterNot(_.trim().isEmpty) match{

      case List(unitFrameDatabaseStr,unitFrameTableNameStr,stratificationPropertiesStr,outputDirectoryStr,unit,bounds) => {
        val params:Seq[scala.util.Try[Any]] = Seq( //
          repository.retrieveTableAsDataFrame(HiveFrame(database = unitFrameDatabaseStr, tableName = unitFrameTableNameStr)),
          Try{spark.read.option(Header, value = true).csv(stratificationPropertiesStr)},
          if(HdfsSupport.exists(new Path(outputDirectoryStr))) Success(new Path(outputDirectoryStr)) else Failure(new IllegalArgumentException(s"output directory: $outputDirectoryStr does not exist")),
          Success(unit),
          Success(bounds)
        )

        val errors = params.foldRight(""){(el, err) => el match{
          case Failure(e) => s"$err ${e.getMessage} \n"
          case Success(df) => err
        }}

        if(!errors.isEmpty) throw new IllegalArgumentException(s"following arguments errors occurred: $errors")
        else SampleMethodsArguments(params.map(_.get))


      }
      case _ => throw new IllegalArgumentException(s"wrong number of arguments: expected 6, actual ${args.length}")
    }

    sma
  }
}
