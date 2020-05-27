package dpla.ingestion3.harvesters.file

import java.io.{BufferedReader, File}

import com.databricks.spark.avro._
import dpla.ingestion3.confs.{Harvest, i3Conf}
import dpla.ingestion3.harvesters.AvroHelper
import dpla.ingestion3.utils.{FlatFileIO, Utils}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.xml._

object WikiMain {

  def main(args: Array[String]): Unit = {
//    val conf: i3Conf = i3Conf(
//      harvest = Harvest(
//        endpoint = Some("/Users/scott/dpla/i3/wiki/original/")
//      )
//    )
//
//    val wiki = new WikiFileHarvester("wiki", conf)
//
//    val harvestedData = wiki.localHarvest()
//
    val outputPath = "/Users/scott/dpla/i3/wiki/harvest/harvestedData.avro"

//    println(s"Harvested count ${harvestedData.count()}")
//
//    println(s"Unique records ${harvestedData.select("id").distinct().count()}")
//    println(s"Top 15 count of images per record \n${harvestedData.groupBy("id").count().sort(desc("count")).show(15, false)}")
//
//    harvestedData
//      .write
//      .format("com.databricks.spark.avro")
//      .option("avroSchema", harvestedData.schema.toString)
//      .mode(SaveMode.Overwrite)
//      .avro(outputPath)
//
//    wiki.cleanUp()


    /**
      *
      */

    val datasetBucket = "dpla-master-dataset"
    val providers = Set("digitalnc")
    val maxTimestamp = "now"
    val masterDataset = new MasterDataset(datasetBucket, providers, maxTimestamp)

    println("Getting paths.")
    val dataPaths: Set[String] = masterDataset.buildPathList("enrichment")

    dataPaths.foreach(println(_))


    val baseConf = new SparkConf()
      .setAppName(s"Harvest: wiki")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "200")
      .set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(baseConf)
      .getOrCreate()

    val wikiAvro = spark.read.avro(outputPath)
    val records: DataFrame = spark.read.format("com.databricks.spark.avro").load(dataPaths.toSeq: _*) // to var args

    println(s"Existing NC records ${records.count()}")
    println(s"Unique records ${wikiAvro.select("id").distinct().count()}")
    println(s"Top 15 count of images per record \n${wikiAvro.groupBy("id").count().sort(desc("count")).show(15, false)}")

    records.printSchema()
    wikiAvro.printSchema()

    // records.join(wikiAvro, "id")
  }
}


class WikiFileHarvester(
                         shortName: String,
                         conf: i3Conf)
  extends Serializable {

  val baseConf = new SparkConf()
    .setAppName(s"Harvest: wiki")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryoserializer.buffer.max", "200")
    .set("spark.master", "local[*]")

  val spark = SparkSession.builder()
    .config(baseConf)
    .getOrCreate()


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


  lazy val schema: Schema =
    new Schema.Parser().parse(new FlatFileIO().readFileAsString("/avro/OriginalRecord.avsc"))

  lazy val avroWriterWiki: DataFileWriter[GenericRecord] = AvroHelper.avroWriter(shortName, tmp, schema)

  // Temporary output path.
  lazy val tmp: String = new File(FileUtils.getTempDirectory, shortName).getAbsolutePath

  def mimeType: String = "application_xml"

  /**
    *
    * @param file
    * @param unixEpoch
    */
  private def harvestFile(file: File, unixEpoch: Long): Unit = {
    println(s"Harvesting from ${file.getName}")

    import spark.implicits._
    val textFile = spark.read.textFile(file.getAbsolutePath) // reads file

    val df = textFile.toDF("record")

    val tmpOut = df.select("record")
      .as[String]
      .rdd
      .map(row => {
        handleLine(row, unixEpoch)
      })
      .collect()
  }

  /**
    * Takes care of parsing an xml file into a list of Nodes each representing an item
    *
    * @param xml Root of the xml document
    * @return List of Options of id/item pairs.
    */
  def handleXML(xml: Node): Option[ParsedResult] = {
    if ((xml \\ "username").text.equalsIgnoreCase("DPLA Bot")) {
      val id = getDplaIdFromTitile( (xml \ "title").text.toString )
      val outputXML = xmlToString(xml)
      Some(ParsedResult(id, outputXML))
    } else {
      None
    }
  }

  /**
    *
    * @param record
    * @param unixEpoch
    * @return
    */
  def handleLine(record: String, unixEpoch: Long): Int = {
    val xml = XML.loadString(record)
    val item = handleXML(xml)

    item match {
      case Some(i) =>
        writeOut(unixEpoch, i)
        1
      case None =>
        0
    }
  }

  def getAvroWriterWiki: DataFileWriter[GenericRecord] = avroWriterWiki

  /**
    * Writes item out
    *
    * @param unixEpoch Timestamp of the harvest
    * @param item Harvested record
    *
    */
  def writeOut(unixEpoch: Long, item: ParsedResult): Unit = {
    val avroWriter = getAvroWriterWiki

    val genericRecord = new GenericData.Record(schema)
    genericRecord.put("id", item.id)
    genericRecord.put("ingestDate", unixEpoch)
    genericRecord.put("provider", shortName)
    genericRecord.put("document", item.item)
    genericRecord.put("mimetype", mimeType)

    avroWriter.append(genericRecord)
  }

  /**
    * Executes the harvest
    */
  def localHarvest(): DataFrame = {
    val harvestTime = System.currentTimeMillis()
    val unixEpoch = harvestTime  / 1000L

    val inFile = new File(conf.harvest.endpoint.getOrElse("in"))

    if (inFile.isDirectory)
      for (file: File <- inFile.listFiles(new Bz2FileFilter)) {
        harvestFile(file, unixEpoch)
      }
    else
      harvestFile(inFile, unixEpoch)

    // flush writes
    avroWriterWiki.flush()

    // Read back avro and return DataFrame
    val tmpOut = spark.read.avro(tmp)
    tmpOut
  }


  /**
    *
    */
  def cleanUp(): Unit = {
    avroWriterWiki.flush()
    avroWriterWiki.close()
    // Delete temporary output directory and files.
    Utils.deleteRecursively(new File(tmp))
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





// ---------------------------------------------------------------------------------------------------------------------

import com.amazonaws.services.s3.{AmazonS3, AmazonS3Client}
import com.amazonaws.services.s3.model.{ListObjectsRequest, ObjectListing}

class MasterDataset(datasetBucket: String, providers: Set[String], maxTimestamp: String) {

  /**
    * Get a data file path for each provider.
    * The file will be in the specified bucket and directory.
    * The file will be the most recent without surpassing the maxTimestamp.
    *
    * @return           Set[String]   The names of the file paths
    */
  def buildPathList(path: String): Set[String] = {
    // List all JSON files
    import scala.collection.JavaConversions._
    val s3: AmazonS3 = new AmazonS3Client()

    def getPrefixes(bucket: String, prefix: String): Seq[String] = {
      def recurse(objects: ObjectListing, acc: List[String]): Seq[String] = {
        val summaries = objects.getCommonPrefixes.toList
        if (objects.isTruncated) {
          s3.listNextBatchOfObjects(objects)
          recurse(objects, acc ::: summaries)
        } else {
          acc ::: summaries
        }
      }

      val request = new ListObjectsRequest()
        .withBucketName(datasetBucket)
        .withDelimiter("/")
        .withPrefix(prefix)
      val objects: ObjectListing = s3.listObjects(request)
      recurse(objects, List())
    }

    val providerList = getPrefixes(datasetBucket, "").toSet

    val providersWhitelist =
      if (providers == Set("all")) providerList
      else providers.map(p => if (p.endsWith("/")) p else s"$p/")

    val filteredProviderList = providerList.intersect(providersWhitelist)

    val dataPaths = filteredProviderList.flatMap(provider =>
      getPrefixes(datasetBucket, f"$provider$path/")
        .filter(key => key.compareTo(f"$provider$path/$maxTimestamp") < 1)
        .sorted
        .lastOption)
      .map(key => f"s3a://$datasetBucket/$key")

    dataPaths

  }
}
