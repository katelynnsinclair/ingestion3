package dpla.ingestion3.harvesters.file


import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.mappers.utils.JsonExtractor
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.jackson.JsonMethods._
import org.json4s.{JValue, _}

import scala.util.{Success, Try}


/**
  * Extracts values from parsed JSON
  */
class JsonlFileExtractor extends JsonExtractor

/**
  * Entry for performing a Florida file harvest
  */
class JsonlFileHarvester(spark: SparkSession,
                      shortName: String,
                      conf: i3Conf,
                      logger: Logger)
  extends FileHarvester(spark, shortName, conf, logger) {

  def mimeType: String = "application_json"

  protected val extractor = new FlFileExtractor()


  /**
    * Parses JValue to extract item local item id and renders compact
    * full record
    *
    * @param json Full JSON item record
    * @return Option[ParsedResult]
    */
  def getJsonResult(json: JValue): Option[ParsedResult] =
    Option(ParsedResult(
      extractor.extractString(json \\ "_id")
        .getOrElse(throw new RuntimeException("Missing ID")),
      compact(render(json))
    ))

  /**
    * Executes the Florida harvest
    */
  override def localHarvest(): DataFrame = {
    val harvestTime = System.currentTimeMillis()
    val unixEpoch = harvestTime / 1000L
    val inFiles = conf.harvest.endpoint.getOrElse("in")

    // Read harvested data into Spark DataFrame.
    import spark.implicits._
    val oringalJson = spark.read.textFile(inFiles)

    oringalJson.foreach(row => {
      val json: JValue = parse(row)
      val i = ParsedResult(
        extractor.extractString(json \\ "_id").getOrElse(throw new RuntimeException("Missing ID")),
        compact(render(json))
      )
      writeOut(unixEpoch, i)
  })

    getAvroWriter.flush()
    val df = spark.read.format("avro").load(tmpOutStr)
    df
  }

  /**
    * Parses and extracts ZipInputStream and writes
    * parses records rootOutput.
    *
    * @param fileResult Case class representing extracted items from a compressed file
    * @return Count of metadata items found.
    */
  override def handleFile(fileResult: FileResult, unixEpoch: Long): Try[Int] = { Success(0) }
}
