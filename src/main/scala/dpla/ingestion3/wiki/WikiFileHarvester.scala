package dpla.ingestion3.wiki

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}
import java.time.LocalDateTime
import java.util.zip.GZIPInputStream

import com.databricks.spark.avro._
import dpla.ingestion3.confs.{CmdArgs, Ingestion3Conf, i3Conf}
import dpla.ingestion3.dataStorage.OutputHelper
import dpla.ingestion3.harvesters.AvroHelper
import dpla.ingestion3.harvesters.file.Bz2FileFilter
import dpla.ingestion3.model
import dpla.ingestion3.model.{ModelConverter, RowConverter}
import dpla.ingestion3.utils.{AvroUtils, FlatFileIO, Utils}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.storage.StorageLevel

import scala.util.{Failure, Success, Try}
import scala.xml._

object WikiMain extends WikiEvaluator {

/**
  * Expects four parameters:
  * 1) a path to the wiki xml dump file(s) preprocessed using xmll
  * 2) a path to output the harvested data
  * 5) spark master (optional parameter that overrides a --master param submitted
  *    via spark-submit)
  *
  * Usage
  * -----
  * To invoke via sbt:
  * sbt "run-main dpla.ingestion3.wiki.WikiMain
  *       --output=/output/path/to/harvest/
  *       --conf=/path/to/conf
  *       --name=shortName"
  *       --sparkMaster=local[*]
  */
  def main(args: Array[String]): Unit = {
    // Read in command line args.
    val cmdArgs = new CmdArgs(args)

    // val dataIn = cmdArgs.getInput
    val dataOut = cmdArgs.getOutput
    val confFile = cmdArgs.getConfigFile
    val shortName = cmdArgs.getProviderName
    val sparkMaster: Option[String] = cmdArgs.getSparkMaster

    // Load configuration from file.
    val i3Conf = new Ingestion3Conf(confFile, Some(shortName))
    val conf: i3Conf = i3Conf.load()

    // Spark configuration
    val baseConf = new SparkConf()
      .setAppName(s"Harvest: wiki")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "200")
      .set("spark.master", sparkMaster.getOrElse("local[*]"))

    // Create SparkSession
    val spark = SparkSession.builder()
      .config(baseConf)
      .getOrCreate()

    // This start time is used for documentation and output file naming.
    val startDateTime = LocalDateTime.now

    val outputHelper: OutputHelper =
      new OutputHelper(dataOut, shortName, "harvest", startDateTime)

    println(s"Saving to ${outputHelper.activityPath}")

    val wiki = new WikiFileHarvester("wiki", conf, spark)

    val harvestedData = wiki.localHarvest()

    println(s"Harvested ${harvestedData.count()} DPLA items from wiki export ")

    // This is duplicating work in WikiFileHarvester
    harvestedData
      .write
      .format("com.databricks.spark.avro")
      .option("avroSchema", harvestedData.schema.toString)
      .mode(SaveMode.Overwrite)
      .avro(outputHelper.activityPath)

    wiki.cleanUp()

    // println(s"Existing NC records ${records.count()}")
    // println(s"Unique records ${wikiAvro.select("id").distinct().count()}")
    // println(s"Top 15 count of images per record \n${wikiAvro.groupBy("id").count().sort(desc("count")).show(15, false)}")

    // TODO which NC records are wiki eligible

    // Fixup to logger
    val wikiAvro = spark.read.avro(outputHelper.activityPath)
    println(s"wiki records count ${wikiAvro.count()}")
  }

  def join(spark: SparkSession) = {

    /**
      *
      */

    val datasetBucket = "dpla-master-dataset"
    val providers = Set("tennessee")
    val maxTimestamp = "now"
    val masterDataset = new MasterDataset(datasetBucket, providers, maxTimestamp)

    println("Getting paths.")
    val dataPaths: Set[String] = masterDataset.buildPathList("enrichment")

    dataPaths.foreach(println(_))

    val records: DataFrame = spark.read.format("com.databricks.spark.avro").load(dataPaths.toSeq: _*) // to var args

    import spark.implicits._
    val dplaMapDataRowEncoder: ExpressionEncoder[Row] = RowEncoder(model.sparkSchema)

    val tupleRowBooleanEncoder: ExpressionEncoder[(Row, Boolean)] =
      ExpressionEncoder.tuple(RowEncoder(model.sparkSchema), ExpressionEncoder())

    val enrichedRows: DataFrame = records // .filter($"dplaUri" === "http://dp.la/api/items/74ed9993f6c9f24fa50960c3b61601b5")

    println("enriched count " + enrichedRows.count())

    val enrichResults: Dataset[(Row, Boolean)] = enrichedRows.map(row => {
      Try{ ModelConverter.toModel(row) } match {
        case Success(dplaMapData) =>

          /**
            * FIXUP
            *
            *  - Change from print to message logger
            *
            */
          val criteria: WikiCriteria = isWikiEligible(dplaMapData)
          (criteria.dataProvider, criteria.asset, criteria.rights) match {
            // All required properties exist
            case (true, true, true) => (RowConverter.toRow(dplaMapData, model.sparkSchema), true)
            // Missing valid standardized rights
            case (true, true, false) =>
              println(s"${dplaMapData.dplaUri.toString} is missing valid rights. Value is ${dplaMapData.edmRights.getOrElse("_MISSING_")}")
              (null, false)
            // Missing assets
            case (true, false, true) =>
              println(s"${dplaMapData.dplaUri.toString} is missing assets.")
              (null, false)
            // Missing dataProvider URI
            case (false, true, true) =>
              println(s"${dplaMapData.dplaUri.toString} is missing dataProvider URI for name '${dplaMapData.dataProvider.name.getOrElse("_MISSSING_")}'")
              (null, false)
            //            // Multiple missing required properties
            case (_, _, _) =>
              //              val mediaMaster = dplaMapData.mediaMaster.map(_.uri.toString).mkString("; ")
              //              println(s"${dplaMapData.dplaUri.toString} is missing multiple requirements. ")
              //                if(!criteria.dataProvider) println(s"\n- dataProvider URI missing for '${dplaMapData.dataProvider.name.getOrElse("")}'")
              //
              //              if(!criteria.asset) println(s"\n- Assets missing for '${dplaMapData.dataProvider.name.getOrElse("")}'")
              //              if(!criteria.dataProvider) println(s"\n- dataProvider URI missing for '${dplaMapData.dataProvider.name.getOrElse("")}'")
              //              if(!criteria.dataProvider) println(s"\n- dataProvider URI missing for '${dplaMapData.dataProvider.name.getOrElse("")}'")
              //              s"\n- edmRights is ${dplaMapData.edmRights.getOrElse("_MISSING_")}" +
              //                s"\n- iiif is ${dplaMapData.iiifManifest.getOrElse("_MISSING_")}" +
              //              )
              (null, false)
          }
        case Failure(err) => (null, false)
      }
    })(tupleRowBooleanEncoder)

    // Filter out only the wiki eligible records
    val wikiRecords = enrichResults
      .filter(tuple => tuple._2)
      .map(tuple => tuple._1)(dplaMapDataRowEncoder)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
  }
}


/**
  *
  * @param shortName
  * @param conf
  * @param spark
  */
class WikiFileHarvester(
                         shortName: String,
                         conf: i3Conf,
                         spark: SparkSession)
  extends Serializable {

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

  println(s"Saving to $tmp")

  def mimeType: String = "application_xml"

  /**
    *
    * @param file
    * @param unixEpoch
    */
  private def harvestFile(file: File, unixEpoch: Long): Unit = {
    println(s"Harvesting from ${file.getName}")
//
//    import spark.implicits._
//    val textFile = spark.read.textFile(file.getAbsolutePath) // reads file
//    val df = textFile.toDF("record")
//
//    df.select("record")
//      .as[String]
//      .rdd
//      .map(row => {
//        handleLine(row, unixEpoch)
//      })
//      .collect()

    val inputStream = getInputStream(file)
      .getOrElse(throw new IllegalArgumentException(s"Couldn't load file, ${file.getAbsolutePath}"))

    val iter = IOUtils.lineIterator(inputStream)

    var lineCount: Int = 0

    while (iter.hasNext) {
      Option(iter.nextLine) match {
        case Some(line) => lineCount += handleLine(line, unixEpoch)
        case None => 0
      }
    }

    IOUtils.closeQuietly(inputStream)

    println(s"Harvested $lineCount lines ")
  }

  def getInputStream(file: File): Option[InputStreamReader] = {
    file.getName match {
      case zipName if zipName.endsWith("bz2") =>
        Some(new InputStreamReader(new BZip2CompressorInputStream(new FileInputStream(file))))
      case _ => None
    }
  }
  /**
    * Takes care of parsing an xml file into a list of Nodes each representing an item
    *
    * @param xml Root of the xml document
    * @return List of Options of id/item pairs.
    */
  def handleXML(xml: Node): Option[ParsedResult] = {
    // if ((xml \\ "username").text.equalsIgnoreCase("DPLA Bot"))
    val id = getDplaIdFromTitile( (xml \ "title").text.toString )
    val outputXML = xmlToString(xml)

    Some(ParsedResult(id, outputXML))
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
    // val avroWriter = getAvroWriterWiki

    val genericRecord = new GenericData.Record(schema)
    genericRecord.put("id", item.id)
    genericRecord.put("ingestDate", unixEpoch)
    genericRecord.put("provider", shortName)
    genericRecord.put("document", item.item)
    genericRecord.put("mimetype", mimeType)

    avroWriterWiki.append(genericRecord)
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
    avroWriterWiki.sync()
    avroWriterWiki.flush()

    // Read back avro and return DataFrame
    val tmpOut = spark.read.avro(tmp)

    println(s"Harvested ${tmpOut.count()} records in `tmp` dataframe")
    tmpOut
  }

  def avroWriter(nickname: String, outputPath: String, schema: Schema): DataFileWriter[GenericRecord] = {
    val filename = s"${nickname}_${System.currentTimeMillis()}.avro"
    val path = if (outputPath.endsWith("/")) outputPath else outputPath + "/"
    val outputDir = new File(path)
    outputDir.mkdirs()
    if (!outputDir.exists) throw new RuntimeException(s"Output directory ${path} does not exist")

    val avroWriter = AvroUtils.getAvroWriter(new File(path + filename), schema)
    avroWriter.setFlushOnEveryBlock(true)
    avroWriter
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

import com.amazonaws.services.s3.model.{ListObjectsRequest, ObjectListing}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3Client}

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


