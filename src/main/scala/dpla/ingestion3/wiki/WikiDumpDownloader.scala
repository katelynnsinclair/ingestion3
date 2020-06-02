package dpla.ingestion3.wiki

import dpla.ingestion3.mappers.utils.JsonExtractor
import dpla.ingestion3.utils.HttpUtils
import org.apache.http.client.utils.URIBuilder

import java.io.File
import java.net.URL

import scala.util.{Failure, Success}

import sys.process._

object WikiDumpDownloader extends JsonExtractor{

  def main(args: Array[String]) = {
    // https://wikimedia.mirror.us.dev/commonswiki/20200520/dumpstatus.json
    val scheme = "https"
    val host = "dumps.wikimedia.your.org"
    val path = "/commonswiki/20200520/dumpstatus.json"
    val downloadFolder = "/Users/scott/dpla/i3/wiki/download/"

    val url = new URIBuilder()
      .setScheme(scheme)
      .setHost(host)
      .setPath(path).build().toURL

    println(s"Checking status at ${url.toString}")


    val json = HttpUtils.makeGetRequest(url) match {
      case Success(j) => org.json4s.jackson.JsonMethods.parse(j)
      case Failure(f) => throw new RuntimeException(s"Unable to load status file b/c ${f.getMessage}")
    }

    val files = extractStrings(json \\ "articlesmultistreamdump" \ "files" \\ "url")
      .filter(file => file.contains(".xml"))

    println(s"Files to download ${files.size}")

    val toDownload = files
      .map(file => new URIBuilder()
        .setScheme(scheme)
        .setHost(host)
        .setPath(file).build().toURL.toString
    )

    toDownload.foreach(println(_))

//    toDownload.foreach(remoteFile => {
//        val filename = remoteFile.substring(remoteFile.lastIndexOf("/"))
//        fileDownloader(remoteFile, downloadFolder + filename)
//      }
//    )

    def fileDownloader(url: String, filename: String) = {
      println(s"Downloading $url to $filename")
      new URL(url) #> new File(filename) !!
    }
  }
}
