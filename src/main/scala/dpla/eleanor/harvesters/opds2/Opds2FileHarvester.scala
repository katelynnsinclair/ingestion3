package dpla.eleanor.harvesters.opds2

import dpla.eleanor.Schemata.{HarvestData, Payload}
import dpla.eleanor.harvesters.Retry
import dpla.eleanor.{HarvestStatics, Schemata}
import org.apache.commons.io.IOUtils
import org.json4s.JValue
import org.json4s.JsonAST.{JArray, JString}
import org.json4s.jackson.JsonMethods

import java.io.{File, FileInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets


trait Opds2FileHarvester extends Retry {

  case class Link(href: String, rel: String, title: String, `type`: String)

  def getId(json: JValue): String =
    (json \ "metadata" \ "identifier") match {
      case JString(x) => x
      case _  => throw new RuntimeException("Missing required ID")
    }

  def harvestFile(file: String, statics: HarvestStatics): Seq[HarvestData] = {
    val inputStream = new InputStreamReader(new FileInputStream(new File(file)))

    // create lineIterator to read contents one line at a time
    val iter = IOUtils.lineIterator(inputStream)

    var rows: Seq[HarvestData] = Seq[HarvestData]()

    while (iter.hasNext) {
      Option(iter.nextLine) match {
        case Some(line) => handleLine(line, statics) match {
          case Some(t) => rows = rows ++: Seq(t)
          case None => rows
        }
        case _ => // do nothing
      }
    }
    IOUtils.closeQuietly(inputStream)

    rows
  }

  def handleLine(line: String, statics: HarvestStatics): Option[HarvestData] =
    Option(line) match {
      case None => None
      case Some(data) =>
        val json = JsonMethods.parse(data)
        val bytes = jsonToBytes(json)
        val id = getId(json)

        Option(HarvestData(
          sourceUri = statics.sourceUri,
          timestamp = statics.timestamp,
          id = id,
          metadataType = statics.metadataType,
          metadata = bytes,
          payloads = getPayloads(json)
        ))
    }

  def getPayloads(record: JValue): Seq[Schemata.Payload] = {

    val images = for {
      JArray(images) <- record \ "images"
      image <- images
      JString(href) = image \ "href"
      JString(typ) = image \ "type"
    } yield {
      Link(href, "", "", typ)
    }

    val links = for {
      JArray(links) <- record \ "links"
      link <- links
      JString(href) = link \ "href"
      JString(rel) = link \ "rel"
      JString(typ) = link \ "type"
    } yield Link(href, rel, "", typ)

    links ++ images map (url => Payload(url = url.href))
  }

  def jsonToBytes(json: JValue): Array[Byte] =
    JsonMethods.compact(JsonMethods.render(json)).getBytes(StandardCharsets.UTF_8)
}
