package dpla.eleanor.harvesters.opds2

import dpla.eleanor.harvesters.Retry
import dpla.eleanor.harvesters.opds2.FeedParser.parsePages
import okhttp3.{Authenticator, Credentials, OkHttpClient, Request, Response, Route}
import org.json4s.JsonAST.JArray
import org.json4s.jackson.JsonMethods
import org.json4s.{JString, JValue}

import java.io.{File, FileWriter, PrintWriter}
import java.net.URL
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}


/**
  * Responsible for harvesting an ebooks feed and writing it out into a file (most likely an XML file)
  *
  * @return The tmp file location
  */

trait Opds2FeedHarvester {

  def harvestFeed(urlStr: String): Try[String] = {

    val timestamp = java.sql.Timestamp.from(Instant.now)

    implicit val outDir = new File(s"${System.getProperty("java.io.tmpdir")}eleanor_$timestamp.json")

    implicit val jsonlOut: PrintWriter = new PrintWriter(
      new FileWriter(
        new File(outDir, "harvest.jsonl")
      )
    )

    implicit val client: OkHttpClient = HttpClientBuilder.getClient(None, None)
    try {
      Try(parsePages(
        new URL(urlStr), 1, pageCallback, publicationCallback
      )).map(_ => outDir.getPath)

    } finally {
      val _ = Try(jsonlOut.close()) //closeQuietly (wish this were Scala 2.13)
    }
  }

  def publicationCallback(json: JValue)(implicit out: PrintWriter): Unit =
    out.println(JsonMethods.compact(json))

  def pageCallback(page: String, pageNum: Int)(implicit dir: File): Unit = {
    val path = new File(dir, f"page-$pageNum%04d.json").toPath
    val bytes = page.getBytes(StandardCharsets.UTF_8)
    val _ = Files.write(path, bytes)
  }
}

object HttpClientBuilder {

  val CONNECT_TIMEOUT = 10L
  val WRITE_TIMEOUT = 10L
  val READ_TIMEOUT = 30L

  def getClient(usernameOption: Option[String], passwordOption: Option[String]): OkHttpClient = {

    val builder = new OkHttpClient.Builder()
      .connectTimeout(CONNECT_TIMEOUT, TimeUnit.SECONDS)
      .writeTimeout(WRITE_TIMEOUT, TimeUnit.SECONDS)
      .readTimeout(READ_TIMEOUT, TimeUnit.SECONDS)

    (usernameOption, passwordOption) match {
      case (Some(user), Some(pass)) =>
        builder.authenticator(new Authenticator() {
          override def authenticate(route: Route, response: Response): Request = {
            response
              .request()
              .newBuilder()
              .header("Authorization", Credentials.basic(user, pass))
              .build()
          }
        })
      case _ =>
    }

    builder.build()
  }
}


object FeedParser extends Retry {

  val RETRY_COUNT = 5

  @tailrec
  def parsePages(
                  location: URL,
                  pageNum: Int,
                  pageCallback: (String, Int) => Unit,
                  publicationCallback: JValue => Unit
                )
                (implicit client: OkHttpClient): Unit = {

    // get page string
    val pageString = retry(RETRY_COUNT) {
      println(s"Requesting ${location.toString}")
      val request = new Request.Builder()
        .url(location)
        .build()
      client
        .newCall(request)
        .execute()
        .body()
        .string()
    } getOrElse (throw new RuntimeException(s"Retries failed for ${location.toString}"))

    // write out page
    pageCallback(pageString, pageNum)

    val json = JsonMethods.parse(pageString)

    // write out publications
    json \ "publications" match {
      case JArray(children) => children.foreach(publicationCallback)
      case _ => throw new Exception("Didn't find publications")
    }

    // recurse on "next" link if available
    getNextLink(json) match {
      case Some(next) =>
        parsePages(
          new URL(next),
          pageNum + 1,
          pageCallback,
          publicationCallback
        )
      case _ => // falls through
    }
  }

  private def getNextLink(json: JValue): Option[String] = (
    for {
      JArray(links) <- json \ "links"
      link <- links
      if link \ "type" == JString("application/opds+json")
      if link \ "rel" == JString("next")
      JString(href) <- link \ "href"
    } yield href).headOption
}
