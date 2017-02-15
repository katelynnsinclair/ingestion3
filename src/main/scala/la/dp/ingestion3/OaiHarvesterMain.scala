package la.dp.ingestion3

import java.io.File
import java.util.concurrent.TimeUnit

import com.databricks.spark.avro.{SchemaConverters, _}
import la.dp.ingestion3.utils.OaiRdd
import org.apache.avro.Schema
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Entry point for running an OAI harvest
  *
  * args Output directory: String
  *             OAI URL: String
  *             Metadata Prefix: String oai_dc oai_qdc, mods, MODS21 etc.
  *             OAI Verb: String ListRecords, ListSets etc.
  *             Provider: Provider name (we need to standardize this)
  */
object OaiHarvesterMain extends App {
  val schemaStr =
    """{
        "namespace": "la.dp.avro",
        "type": "record",
        "name": "OriginalRecord.v1",
        "doc": "",
        "fields": [
          {"name": "id", "type": "string"},
          {"name": "provider", "type": "string"},
          {"name": "document", "type": "string"},
          {"name": "mimetype", "type": { "name": "MimeType",
           "type": "enum", "symbols": ["application_json", "application_xml", "text_turtle"]}
           }
        ]
      }
    """//.stripMargin //todo we need to template the document field so we can record info there

  val logger = LogManager.getLogger(OaiHarvesterMain.getClass)

  // Complains about not being typesafe...
  if(args.length != 5 ) {
    logger.error("Bad number of args: <OUTPUT FILE>, <OAI URL>, <METADATA PREFIX>, <OAI VERB>, <PROVIDER>")
    sys.exit(-1)
  }

  println(schemaStr)

  val urlBuilder = new OaiQueryUrlBuilder
  val outputFile = args(0)
  val oaiParams = Map[String,String](
    "endpoint" -> args(1),
    "metadataPrefix" -> args(2),
    "verb" -> args(3))

  val provider = args(4)

  val sparkConf = new SparkConf()
    .setAppName("Oai Harvest")
    .setMaster("local") //todo parameterize

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  val sc = spark.sparkContext

  val avroSchema = new Schema.Parser().parse(schemaStr)
  val schemaType = SchemaConverters.toSqlType(avroSchema)
  val structSchema = schemaType.dataType.asInstanceOf[StructType]
  val oaiRdd: OaiRdd = new OaiRdd(sc, oaiParams, urlBuilder)
  val rows = oaiRdd.map(data => Row(data._1, provider, data._2, "application_xml"))
  val dataframe = spark.createDataFrame(rows, structSchema).limit(100)
  dataframe.write.format("com.databricks.spark.avro").option("avroSchema", schemaStr).avro(outputFile)
  sc.stop()

  //TODO: should probably do this by loading the avro into a new dataframe and calling count()
  getAvroCount(new File(outputFile + "/part-00000.deflate"))

  /**
    * Print the results of a harvest
    *
    * Example:
    *   Harvest count: 242924 records harvested
    *   Runtime: 4 minutes 24 seconds
    *   Throughput: 920 records/second
    *
    * @param runtime Runtime in milliseconds
    * @param recordsHarvestedCount Number of records in the output directory
    */

  def printResults(runtime: Long, recordsHarvestedCount: Long): Unit = {
    // Make things pretty
    val formatter = java.text.NumberFormat.getIntegerInstance
    val minutes: Long = TimeUnit.MILLISECONDS.toMinutes(runtime)
    val seconds: Long = TimeUnit.MILLISECONDS.toSeconds(runtime) -
      TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(runtime))
    val runtimeInSeconds: Long = TimeUnit.MILLISECONDS.toSeconds(runtime) + 1
    // add 1 to avoid divide by zero error
    val recordsPerSecond: Long = recordsHarvestedCount/runtimeInSeconds

    println(s"File count: ${formatter.format(recordsHarvestedCount)}")
    println(s"Runtime: $minutes:$seconds")
    println(s"Throughput: ${formatter.format(recordsPerSecond)} records/second")
  }

  /**
    * Counts the number of items in a Avro file
    *
    * @param path File
    *             Path to the Avro file
    * @return Integer
    *         The count of items
    */
  def getAvroCount(path: File): Integer = {
    -1
  }
}
