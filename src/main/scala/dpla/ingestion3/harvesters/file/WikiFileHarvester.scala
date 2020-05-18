package dpla.ingestion3.harvesters.file

import java.io.{BufferedReader, File, FileInputStream}
import java.util.zip.GZIPInputStream

import com.databricks.spark.avro._
import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.{AvroHelper, Harvester}
import dpla.ingestion3.utils.{FlatFileIO, Utils}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.tools.bzip2.CBZip2InputStream
import org.apache.tools.tar.TarInputStream

import scala.util.{Failure, Success, Try}
import scala.xml._

class WikiFileHarvester(
                         spark: SparkSession,
                         shortName: String,
                         conf: i3Conf,
                         logger: Logger)
  extends Harvester(spark, shortName, conf, logger) {

  /**
    * Case class hold the parsed value from a given FileResult
    */
  case class ParsedResult(id: String, item: String)


  /**
    * Case class to hold the results of a file
    *
    * @param entryName    Path of the entry in the file
    * @param data         Holds the data for the entry, or None if it's a directory.
    * @param bufferedData Holds a buffered reader for the entry if it's too
    *                     large to be held in memory.
    */
  case class FileResult(entryName: String,
                        data: Option[Array[Byte]],
                        bufferedData: Option[BufferedReader] = None)


  lazy val naraSchema: Schema =
    new Schema.Parser().parse(new FlatFileIO().readFileAsString("/avro/OriginalRecord.avsc"))

  val avroWriterNara: DataFileWriter[GenericRecord] =
    AvroHelper.avroWriter(shortName, naraTmp, naraSchema)

  // Temporary output path.
  lazy val naraTmp: String = new File(FileUtils.getTempDirectory, shortName).getAbsolutePath

  def mimeType: String = "application_xml"

  /**
    * Loads .gz, .tgz, .bz, and .tbz2, and plain old .tar files.
    *
    * @param file File to parse
    * @return TarInputstream of the tar contents
    */
  def getInputStream(file: File): Option[TarInputStream] =
    file.getName match {
      case gzName if gzName.endsWith("gz") || gzName.endsWith("tgz") =>
        Some(new TarInputStream(new GZIPInputStream(new FileInputStream(file))))

      case bz2name if bz2name.endsWith("bz2") || bz2name.endsWith("tbz2") =>
        val inputStream = new FileInputStream(file)
        inputStream.skip(2)
//        Some(new TarArchiveInputStream(new CBZip2InputStream(new BufferedInputStream(inputStream))))
         Some(new TarInputStream(new CBZip2InputStream(inputStream)))

      case tarName if tarName.endsWith("tar") =>
        Some(new TarInputStream(new FileInputStream(file)))

      case _ => None
    }

  /**
    * Takes care of parsing an xml file into a list of Nodes each representing an item
    *
    * @param xml Root of the xml document
    * @return List of Options of id/item pairs.
    */
  def handleXML(xml: Node): List[Option[ParsedResult]] = {
    for {
      //three different types of nodes contain children that represent records
      items <- xml \\ "page" :: Nil
      item <- items
      if (item \ "revision" \ "contributor" \ "username").text.equalsIgnoreCase("DPLA Bot")
    } yield item match {
      case record: Node =>
        val id = (record \ "title").text.toString
        val outputXML = xmlToString(record)
        val label = item.label
        println(id)
        Some(ParsedResult(id, outputXML))
      case _ =>
        logger.warn("Got weird result back for item path: " + item.getClass)
        None
    }
  }

  /**
    * Main logic for handling individual entries in the tar.
    *
    * @param tarResult  Case class representing extracted item from the tar
    * @return Count of metadata items found.
    */
  def handleFile(record: String,
                 unixEpoch: Long,
                 filename: String): Try[Int] = {
        Try {
          val dataString = record.replaceAll("<\\?xml.*\\?>", "").trim
          val xml = XML.loadString(dataString)

          println(xml.toString())
          val items = handleXML(xml)

          val counts = for {
            itemOption <- items
            item <- itemOption // filters out the Nones
          } yield {
            writeOutNara(unixEpoch, item, filename)
            1
          }
          counts.sum
        }
    }

  def getAvroWriterNara: DataFileWriter[GenericRecord] = avroWriterNara

  /**
    * Writes item out
    *
    * @param unixEpoch Timestamp of the harvest
    * @param item Harvested record
    *
    */
  def writeOutNara(unixEpoch: Long, item: ParsedResult, file: String): Unit = {
    val avroWriter = getAvroWriterNara
    val schema = naraSchema

    val genericRecord = new GenericData.Record(schema)
    genericRecord.put("id", item.id)
    genericRecord.put("ingestDate", unixEpoch)
    genericRecord.put("provider", shortName)
    genericRecord.put("document", item.item)
    genericRecord.put("mimetype", mimeType)

    avroWriter.append(genericRecord)

  }

  /**
    * Implements a stream of files from the tar.
    * Can't use @tailrec here because the compiler can't recognize it as tail recursive,
    * but this won't blow the stack.
    *
    * @param tarInputStream
    * @return Lazy stream of tar records
    */
  def iter(tarInputStream: TarInputStream): Stream[FileResult] =
    Option(tarInputStream.getNextEntry) match {
      case None =>
        Stream.empty

      case Some(entry) =>
        val filename = Try {
          entry.getName
        }.getOrElse("")

        val result =
          if (entry.isDirectory || filename.contains("._")) // drop OSX hidden files
            None
          else if (filename.contains(".xml")) // only read xml files
            Some(IOUtils.toByteArray(tarInputStream, entry.getSize))
          else
            None

        FileResult(entry.getName, result) #:: iter(tarInputStream)
    }

  /**
    * Executes the nara harvest
    */
  def localHarvest(): DataFrame = {
    val harvestTime = System.currentTimeMillis()
    val unixEpoch = harvestTime  / 1000L

    logger.info(s"Writing harvest tmp output to $naraTmp")

    // FIXME This assumes files on local file system and not on S3. Files should be able to read off of S3.
    val inFile = new File(conf.harvest.endpoint.getOrElse("in"))

    if (inFile.isDirectory)
      for (file: File <- inFile.listFiles) {
        logger.info(s"Harvesting from ${file.getName}")
        harvestFile(file, unixEpoch)
      }
    else
      harvestFile(inFile, unixEpoch)

    // flush writes
   avroWriterNara.flush()

    // Get the absolute path of the avro file written to naraTmp directory. copyFromLocalFile() cannot copy
    // a directory
    val naraTempFile = new File(naraTmp)
      .listFiles(new AvroFileFilter)
      .headOption
      .getOrElse(throw new RuntimeException(s"Unable to load avro file in $naraTmp directory. Unable to continue"))
      .getAbsolutePath

    val localSrcPath = new Path(naraTempFile)
    val dfAllRecords = spark.read.avro(localSrcPath.toString)

    dfAllRecords

  }


  override def cleanUp(): Unit = {
    logger.info(s"Cleaning up $naraTmp directory and files")
    avroWriterNara.flush()
    avroWriterNara.close()
    // Delete temporary output directory and files.
    Utils.deleteRecursively(new File(naraTmp))
  }

  private def harvestFile(file: File, unixEpoch: Long): Unit = {
//    val inputStream = getInputStream(file)
//      .getOrElse(throw new IllegalArgumentException(s"Couldn't load tar file: ${file.getAbsolutePath}"))

    println(s"Read from ${file.getAbsolutePath}")
    val fileText = spark.read.textFile(file.getAbsolutePath) // reads uncompressed file
    println(s"count ${fileText.count()}")

    import spark.implicits._
    val filtered = fileText
      .map(string => XML.loadString(string).asInstanceOf[NodeSeq])
      .filter(node => (node \\ "revision" \ "contributor" \ "username").text.equalsIgnoreCase("DPLA Bot"))
      .map(node => {
        val title = (node \\ "title").text
        title.split(" - ").last.take(32)
      })

    println(s"Filtered ${filtered.count()}")
    println (s"Distinct ${filtered.distinct().count()}")
//
//    val dplaIds = filtered.map( x => {
//      val title = (XML.loadString(x) \\ "title").text
////      File:The Almanac (1902) - DPLA - 946db18eacfdd76d8a257fc8443184c1 (page 49).jpg
////      File:Albert F. Graf letter to Agnes H. Petersen, June 25, 1918 - DPLA - 740985c0712b257f733569fadc2505eb (page 6).jpg
////      File:Block Card 1818 Giant Street - DPLA - 4c13c5f9d2046bbe8d8631a3255d1917.jpg
////      File:Block Card 1202 Vance Street - DPLA - 7409bcc7a31bf8f91c49d27e04db5013.jpg
////      File:The Almanac (1902) - DPLA - 946db18eacfdd76d8a257fc8443184c1 (page 50).jpg
//      title.split(" - ").last.take(32)
//    })



//    val fileXml = fileText.map(record => {
//      logger.info(s"record == $record")
//      // handleFile(record, unixEpoch, file.getName)
//      Row(XML.loadString(record))
////       match {
////        case Failure(exception) =>
////          logger.error(s"Caught exception on ${record}.", exception)
////          // 0
////        case Success(count) =>
////          // count
////      }
//    }).toDF()

    // logger.info(s"Harvested $recordCount records from ${file.getName}")
  }

  /**
    * Converts a Node to an xml string
    *
    * @param node The root of the tree to write to a string
    * @return a String containing xml
    */
  def xmlToString(node: Node): String =
    Utility.serialize(node, minimizeTags = MinimizeMode.Always).toString


  def getDplaIdFromTitile(string: String): String = {
    string.split(" - ").last.take(32)
  }
}