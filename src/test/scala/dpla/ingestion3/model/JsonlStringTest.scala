package dpla.ingestion3.model


import dpla.ingestion3.data.EnrichedRecordFixture
import org.json4s.JsonAST.{JString, JValue, JField}
import org.json4s._
import org.json4s.native.JsonMethods._
import org.scalatest.FlatSpec

class JsonlStringTest extends FlatSpec {

  "jsonlRecord" should "print a valid JSON string" in {
     val s: String = jsonlRecord(EnrichedRecordFixture.enrichedRecord)
     val jvalue = parse(s)
    assert(jvalue.isInstanceOf[JValue])
  }

  it should "render a field that's a String" in {
    val s: String = jsonlRecord(EnrichedRecordFixture.enrichedRecord)
    val jvalue = parse(s)
    assert(
      toJsonString(jvalue \ "_source" \ "id") ==
        "\"4b1bd605bd1d75ee23baadb0e1f24457\""
    )
  }

  it should "render a field that's a sequence" in {
    val s: String = jsonlRecord(EnrichedRecordFixture.enrichedRecord)
    val jvalue = parse(s)
    val title = jvalue \ "_source" \ "sourceResource" \ "title"
    assert(title.isInstanceOf[JArray])
    assert(toJsonString(title(0)) == "\"The Title\"")
  }

  it should "render a field that requires a map() on a sequence" in {
    val s: String = jsonlRecord(EnrichedRecordFixture.enrichedRecord)
    val jvalue = parse(s)
    val collection = jvalue \ "_source" \ "sourceResource" \ "collection"
    assert(collection.isInstanceOf[JArray])
    assert(
      toJsonString(collection(0)) == "{\"title\":\"The Collection\",\"description\":\"The Archives of Some Department, U. of X\"}"
    )
  }

  it should "render a iiifManfiest " in {
    val s: String = jsonlRecord(EnrichedRecordFixture.enrichedRecord)
    val jvalue = parse(s)
    val iiifManifest = jvalue \ "_source" \ "iiifManifest"
    assert(iiifManifest.isInstanceOf[JString])
    assert(
      toJsonString(iiifManifest) == "\"https://ark.iiif/item/manifest\""
    )
  }

// FIXME HOW DOES THIS TEST WORK?
  
//  it should "for fields that have no data" in {
//    // Those fields that are optional are 0-n, so they will be arrays.
//    val s: String = jsonlRecord(EnrichedRecordFixture.minimalEnrichedRecord)
//    val jvalue = parse(s)
//    val actual = toJsonString(jvalue \ "_source" \ "sourceResource" \ "collection")
//    assert(actual === "")
//  }

  it should "use the same ingestDate across multiple calls" in {
    val s1: String = ingestDate
    Thread.sleep(100)
    val s2: String = ingestDate
    assert(s1 == s2)
  }

  "ingestDate" should "return a string in the correct format" in {
    assert(ingestDate matches
           """^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z$""")
  }

}
