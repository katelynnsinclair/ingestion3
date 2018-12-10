package dpla.ingestion3.harvesters.api

import java.io.File
import java.net.URL

import com.databricks.spark.avro._
import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.{AvroHelper, Harvester}
import dpla.ingestion3.utils.{HttpUtils, Utils}
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.commons.io.FileUtils
import org.apache.http.client.utils.URIBuilder
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}
import scala.xml.XML

/**
  * Class for harvesting records from the Library of Congress's API
  *
  * API documentation
  * https://libraryofcongress.github.io/data-exploration/
  *
  */
class LocHarvester(spark: SparkSession,
                   shortName: String,
                   conf: i3Conf,
                   logger: Logger)
  extends Harvester(spark, shortName, conf, logger) with Serializable {

  def mimeType: String = "application_json"

  protected val queryParams: Map[String, String] = Map(
    "query" -> conf.harvest.query,
    "c" -> conf.harvest.rows
  ).collect{ case (key, Some(value)) => key -> value } // remove None values


  /**
    * Returns set names from configuration file
    *
    * @return Array[String] Collection names
    * @throws RuntimeException If no sets are defined in configuration file
    */
  def getCollections: Array[String] = conf.harvest.setlist
    .getOrElse(throw new RuntimeException("No sets")).split(",")

  /**
    * Entry method for invoking LC harvest
    */
  override def localHarvest(): DataFrame = {
    // Get sets from conf
    val collections = getCollections

    var itemUrls: ListBuffer[String] = new ListBuffer[String]()

    collections.foreach(collection => {
      // Mutable vars for controlling harvest loop
      var continueHarvest = true
      var page = "1"

      logger.info(s"Processing sitemaps for collection: $collection")

      while (continueHarvest) getSinglePage(page, collection) match {
        // Handle errors
        case error: ApiError with ApiResponse =>
          logError(error)
          continueHarvest = false
        // Handle a successful response
        case src: ApiSource with ApiResponse => src.text match {
          case Some(docs) =>
            val xml = XML.loadString(docs)
            val locItemNodes = xml \\ "url" \\ "loc"
            // Extract URLs from site map, filter out non-harvestable URLs and
            // append valid URLs to itemUrls List
            itemUrls = itemUrls ++ locItemNodes
              .filter(url => url.text.contains("www.loc.gov/item/"))
              .map(node => buildItemUrl(node.text))
            // Loop control
            if (locItemNodes.size != queryParams.getOrElse("c", "10").toInt)
              continueHarvest = false
            else
              page = (page.toInt + 1).toString
          // Handle empty response from API
          case _ =>
            logError(ApiError("Response body is empty", src))
            continueHarvest = false
        }
      }
    })

    // Log results of item gathering
    logger.info(s"Collected ${itemUrls.size} urls from collection pages")

    // Check for and report duplicate item urls in feed
    if (itemUrls.distinct.size != itemUrls.size)
      logger.info(s"${itemUrls.distinct.size} distinct values. " +
        s"${itemUrls.size - itemUrls.distinct.size} duplicates}")

    val parallelUrls = spark.sparkContext.parallelize(itemUrls.distinct)

    val fetchUrl = (url: String) => HttpUtils.makeGetRequest(new URL(url)) match {
      case Failure(e) =>

      //        saveOutErrors(List(
      //          ApiError(e.getStackTrace.mkString("\n\t"),
      //            ApiSource(emptyParams, Some(myDumbUrl), None))
      //        ))

      case Success(response) =>
        Try {
          parse(response)
        } match {
          case Success(json) =>

            val recordId = (json \\ "item" \\ "id").toString

            if (recordId.nonEmpty)
              ApiRecord(recordId, compact(render(json)))
          //            else
          //              saveOutErrors(List(
          //                ApiError("Missing required property 'id'",
          //                  ApiSource(emptyParams, Some(myDumbUrl), Some(compact(render(json)))))
          //              ))

          case Failure(parseError) =>

          //            saveOutErrors(List(
          //              ApiError(parseError.getStackTrace.mkString("\n\t"),
          //                ApiSource(emptyParams, Some(myDumbUrl), Some(response)))
          //            ))

        }
    }

    val afterUrls = parallelUrls.map(fetchUrl)

    logger.info("Fetched items ")

    val locRecords = afterUrls.collect { case a: ApiRecord => a }

    logger.info("Filtered for ApiRecords")

    // @see ApiHarvester

    saveOutRecords(locRecords.collect().toList)

    logger.info("Saved out")

    // Read harvested data into Spark DataFrame and return.
    spark.read.avro(tmpOutStr)
  }

  /**
    * Get a single-page, un-parsed response from the CDL feed, or an error if
    * one occurs.
    *
    * @param sp Pagination
    * @param collection Name of collection
    * @return ApiSource or ApiError
    */
  def getSinglePage(sp: String, collection: String): ApiResponse = {
    val url = buildCollectionUrl(queryParams.updated("sp", sp).updated("collection", collection))
    HttpUtils.makeGetRequest(url) match {
      case Failure(e) =>
        ApiError(e.toString, ApiSource(queryParams, Some(url.toString)))
      case Success(response) => if (response.isEmpty) {
        ApiError("Response body is empty", ApiSource(queryParams, Some(url.toString)))
      } else {
        ApiSource(queryParams, Some(url.toString), Some(response))
      }
    }
  }

  /**
    * Constructs the URL for collection sitemamp requests
    *
    * @param params URL parameters
    * @return
    */
  def buildCollectionUrl(params: Map[String, String]): URL =
    new URIBuilder()
      .setScheme("http")
      .setHost("www.loc.gov")
      .setPath(s"/collections/${params.getOrElse("collection",
        throw new RuntimeException("No collection specified"))}")
      .setParameter("c", params.getOrElse("c", "10"))
      .setParameter("fo", "sitemap")
      .setParameter("sp", params.getOrElse("sp", "1"))
      .build()
      .toURL

  /**
    * Constructs a URL to request the JSON view of an item in LC's API
    *
    * @param urlStr String
    * @return URL
    */
  def buildItemUrl(urlStr: String): String = {
    val url = new URL(urlStr)

    new URIBuilder()
      .setScheme(url.getProtocol)
      .setHost(url.getHost)
      .setPath(url.getPath)
      .setParameter("fo", "json")
      .setParameter("at", "item")
      .build()
      .toURL
      .toString
  }

  /**
    * Log error messages
    *
    * @param error ApiError
    * @param msg Error message
    */
  def logError(error: ApiError, msg: Option[String] = None): Unit = {
    logger.error("%s  %s\n%s\n%s".format(
      msg.getOrElse("URL: "),
      error.errorSource.url.getOrElse("!!! Undefined URL !!!"),
      error.errorSource.queryParams,
      error.message
    ))
  }


  val tmpOutStr: String = new File(FileUtils.getTempDirectory, shortName).getAbsolutePath

  // Delete temporary output directory and files if they already exist.
  Utils.deleteRecursively(new File(tmpOutStr))

  private val avroWriter: DataFileWriter[GenericRecord] =
    AvroHelper.avroWriter(shortName, tmpOutStr, Harvester.schema)


  def getAvroWriter: DataFileWriter[GenericRecord] = avroWriter


  /**
    *
    * @param docs
    */
  def saveOutRecords(docs: List[ApiRecord]) = {

    val avroWriter = getAvroWriter

    // TODO Integrate this with File harvester save out methods
    docs.foreach(doc => {
      val startTime = System.currentTimeMillis()
      val unixEpoch = startTime / 1000L

      val genericRecord = new GenericData.Record(Harvester.schema)

      genericRecord.put("id", doc.id)
      genericRecord.put("ingestDate", unixEpoch)
      genericRecord.put("provider", shortName)
      genericRecord.put("document", doc.document)
      genericRecord.put("mimetype", mimeType)

      avroWriter.append(genericRecord)
    })
  }
  /**
    * Writes errors out to log file
    *
    * @param errors List[ApiErrors}
    */
  def saveOutErrors(errors: List[ApiError]): Unit =
    errors.foreach(error => {
      logger.error(s"URL: ${error.errorSource.url.getOrElse("No url")}" +
        s"\nMessage: ${error.message} \n\n")
    })
}
