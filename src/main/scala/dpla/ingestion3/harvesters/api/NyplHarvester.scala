
package dpla.ingestion3.harvesters.api

import java.net.URL

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.mappers.xml.XmlExtractionUtils
import dpla.ingestion3.utils.HttpUtils
import org.apache.http.client.utils.URIBuilder
import org.apache.log4j.Logger
import org.json4s.DefaultFormats

import scala.util.{Failure, Success}
import scala.xml.XML

/**
  * Class for harvesting records from the New York Public Library API
  *
  * Calisphere API documentation
  * https://help.oac.cdlib.org/support/solutions/articles/9000101639-calisphere-api
  *
  */
class NyplHarvester(shortName: String,
                    conf: i3Conf,
                    outputDir: String,
                    harvestLogger: Logger)
  extends ApiHarvester(shortName, conf, outputDir, harvestLogger)
    with XmlExtractionUtils with Serializable {

  // URL that will list all the set ids to harvest
  protected val setsUrl = new URL("http://api.repo.nypl.org/api/v1/items/roots.xml")

  // For API Authorization
  protected val httpHeaders = Option(Map("Authorization" ->
    s"Token token=${conf.harvest.apiKey.getOrElse(
      throw new RuntimeException("Missing API key"))}"))

  override protected val mimeType: String = "application_xml"

  override protected val queryParams: Map[String, String] = Map(
    "set" -> "",
    "page" -> "1",
    "per_page" -> conf.harvest.rows.getOrElse("10")
  )



  /**
    * Primary driver of the harvest
    */
  override protected def localApiHarvest: Unit = {
    implicit val formats = DefaultFormats

    // These calls will generate a list of item calls to be made.
    val pageCalls = computeAllPageCalls()

    harvestLogger.info(s"Total number of page calls ${pageCalls.size}")
    println(s"Total number of page calls ${pageCalls.size}")

    // A list of 2M items
    val itemCalls = executePageCalls(pageCalls)

    harvestLogger.info(s"Total number of item calls ${itemCalls.size}")

    val itemRdd = sc.parallelize(itemCalls)

    val thisHttpHeaders = httpHeaders

    val execItem = (url: URL) => {
      HttpUtils.makeGetRequest(url, thisHttpHeaders) match {
        case Success(rsp) =>
          ApiRecord(url.toString, rsp.toString) // FIXME bad item id, calling saveOut on every record
        case Failure(fail) =>
          ApiError(s"${fail.getMessage}\n${fail.getStackTrace.mkString("\n")}\n",
            ApiSource(Map("" -> ""), Some(url.toString)))
      }
    }

    val out = itemRdd.map(execItem)

    val success = out
      .filter(_.isInstanceOf[ApiRecord])
        .map(_.asInstanceOf[ApiRecord])
        .collect()
        .toList

    val errors = out
      .filter(_.isInstanceOf[ApiError])
      .map(_.asInstanceOf[ApiError])
      .collect()
      .toList

    harvestLogger.info(s"Successful ${success.size}")
    harvestLogger.info(s"Errors ${errors.size}")

    errors.map(error => harvestLogger.error(s"${error.errorSource.url.get} -- ${error.message}\n"))

    saveOut(success)
  }


  /**
    *
    * @param pageCalls
    * @return
    */
  def executePageCalls(pageCalls: Seq[URL]): Seq[URL] = {
    pageCalls.map(pageUrl => {
      HttpUtils.makeGetRequest(pageUrl, httpHeaders) match {
        case Failure(e) =>
          harvestLogger.error(e.getMessage)
        case Success(response) =>
          if (response.isEmpty) {
            harvestLogger.error("Empty body")
          } else {
            response
          }
        }
      }
    ).collect {
      case p: String =>
        val xml = XML.loadString(p)
        extractStrings(xml \ "response" \ "capture" \ "uuid").distinct.map(buildItemUrl)
      }.flatten
  }

  /**
    * Get a single-page, un-parsed response from the NYPL feed, or an error if
    * one occurs.
    *
    * @return ApiSource or ApiError
    */
  private def getSinglePage(set: String, page: String): ApiResponse = {
    val updateQryParams = queryParams.updated("set", set).updated("page", page)
    val thisHttpHeader = httpHeaders
    val url = buildUrl(updateQryParams)

    HttpUtils.makeGetRequest(url, thisHttpHeader) match {
      case Failure(e) =>
        ApiError(e.getMessage, ApiSource(updateQryParams, Some(url.toString)))
      case Success(response) => if (response.isEmpty) {
        ApiError("Response body is empty", ApiSource(updateQryParams, Some(url.toString)))
      } else {
        ApiSource(updateQryParams, Some(url.toString), Some(response))
      }
    }
  }

  /**
    * Constructs the URL for requesting set pages
    * http://api.repo.nypl.org/api/v1/items/
    *
    * @param params URL parameters
    * @return Constructed URL
    */
  override protected def buildUrl(params: Map[String, String]): URL =
    new URIBuilder()
      .setScheme("http")
      .setHost("api.repo.nypl.org")
      .setPath(s"/api/v1/items/${params.getOrElse("set", throw new RuntimeException("No set given"))}.xml")
      .setParameter("page", params.getOrElse("page", "1"))
      .setParameter("per_page", params.getOrElse("per_page", "10"))
      .build()
      .toURL

  /**
    * Builds a URL for requesting an item page
    * http://api.repo.nypl.org/api/v1/items/mods/____.xml
    *
    * @param itemId Item ID to request
    * @return Constructed URL
    */
  protected def buildItemUrl(itemId: String): URL =
    new URIBuilder()
      .setScheme("http")
      .setHost("api.repo.nypl.org")
      .setPath(s"/api/v1/items/mods/$itemId.xml")
      .build()
      .toURL

  /**
    * Get all set ids
    */
  protected def getSetIds: Seq[String] = {
    val response = HttpUtils.makeGetRequest(setsUrl, httpHeaders) match {
      case Success(rsp) => XML.loadString(rsp)
      case Failure(f) => throw f
    }
    extractStrings(response \\ "uuid").takeRight(4000)
  }

  /**
    *
    * @return 
    */
  protected def getSetPageCounts(): Seq[(String, Int)] = {
    // Set ids to test
    // eaa9d570-c6bf-012f-0446-58d385a7bc34 -- 1732 items -- ISSUES
    // ebef8ed0-1842-0133-5142-58d385a7bbd0 -- 202 items
    // a71baeb0-c5b2-012f-595e-58d385a7bc34 -- 2283 items
    // 4b7ff3c0-577a-0130-1851-58d385a7b928 -- 457 items


    // println(HttpUtils.makeGetRequest(new URL("")))

//    val setIds = Seq(
//      "4b7ff3c0-577a-0130-1851-58d385a7b928",
//      "a71baeb0-c5b2-012f-595e-58d385a7bc34",
//      "ebef8ed0-1842-0133-5142-58d385a7bbd0")

    val setIds = getSetIds

    val setCounts = setIds.map(set => {
      getSinglePage(set, "1") match {
        case src: ApiSource with ApiResponse =>
          val xml = XML.loadString(src.text.get)
          val pages = new Integer((xml \\ "totalPages").text).intValue()
          set -> pages
      }
    })
    setCounts
  }

  /**
    *
    * @return
    */
  protected def computeAllPageCalls(): Seq[URL] = {
    val setPageCounts = getSetPageCounts()

    setPageCounts.flatMap{ case (set, pageCount) => {
      List.fill(pageCount)(set).zipWithIndex.map { case (s, i) => {
        buildUrl(queryParams.updated("set", set).updated("page", (i+1).toString)) // (i+1) kludge
      }}
    }}
  }
}
