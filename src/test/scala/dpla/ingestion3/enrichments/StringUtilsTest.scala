package dpla.ingestion3.enrichments

import org.scalatest.{BeforeAndAfter, FlatSpec}
import dpla.ingestion3.enrichments.StringUtils._

class StringUtilsTest extends FlatSpec with BeforeAndAfter {

  "convertToSentenceCase" should "capitalize the first character in each " +
    "sentence." in {
    val originalValue = "this is a sentence about Moomins. this is another about Snorks."
    val enrichedValue = originalValue.convertToSentenceCase
    val expectedValue = "This is a sentence about Moomins. This is another about Snorks."

    assert(enrichedValue === expectedValue)
  }

  "splitAtDelimiter" should "split a string around semi-colon" in {
    val originalValue = "subject-one; subject-two; subject-three"
    val enrichedValue = originalValue.splitAtDelimiter(";")
    val expectedValue = Array("subject-one", "subject-two", "subject-three")

    assert(enrichedValue === expectedValue)
  }

  "splitAtDelimiter" should "split a string around comma." in {
    val originalValue = "subject-one, subject-two; subject-three"
    val enrichedValue = originalValue.splitAtDelimiter(",")
    val expectedValue = Array("subject-one", "subject-two; subject-three")

    assert(enrichedValue === expectedValue)
  }

  "stripHMTL" should "remove html from a string" in {
    val expectedValue = "foo bar baz buzz"
    val originalValue = f"<p>$expectedValue%s</p>"
    val enrichedValue = originalValue.stripHTML
    assert(enrichedValue === expectedValue)
  }

  it should "handle strings with unbalanced and invalid html" in {
    val expectedValue = "foo bar baz buzz"
    val originalValue = f"<p>$expectedValue%s</i><html>"
    val enrichedValue = originalValue.stripHTML
    assert(enrichedValue === expectedValue)
  }

  it should "passthrough strings that do not contain html" in {
    val expectedValue = "foo bar baz buzz"
    val originalValue = expectedValue
    val enrichedValue = originalValue.stripHTML
    assert(enrichedValue === expectedValue)
  }

  it should "not emit HTML entities" in {
    val expectedValue = "foo bar baz > buzz"
    val originalValue = expectedValue
    val enrichedValue = originalValue.stripHTML
    assert(enrichedValue === expectedValue)
  }

  it should "not turn html entities into html" in {
    val originalValue = "foo bar baz &lt;p&gt; buzz"
    val expectedValue = "foo bar baz  buzz"
    val enrichedValue = originalValue.stripHTML
    assert(enrichedValue === expectedValue)
  }

  "stripPunctuation" should "strip all punctuation from the given string" in {
    val originalValue = "\t\"It's @#$,.! OK\"\n"
    val enrichedValue = originalValue.stripPunctuation
    val expectedValue = "\t\"It's  OK\"\n"
    assert(enrichedValue === expectedValue)
  }

  "stripLeadingPunctuation" should "strip leading punctuation from the " +
      "given string" in {
    val originalValue = "@#$,.!\t \"It's OK\""
    val enrichedValue = originalValue.stripPunctuation
    val expectedValue = "\t \"It's OK\""
    assert(enrichedValue === expectedValue)
  }

  "stripEndingPunctuation" should "strip ending punctuation from the " +
    "given string" in {
    val originalValue = "\"It's OK\" @#$,.!"
    val enrichedValue = originalValue.stripPunctuation
    val expectedValue = "\"It's OK\" "
    assert(enrichedValue === expectedValue)
  }

  "limitCharacters" should "limit the number of characters in long strings" in {
    val longString = "Now is the time for all good people to come to the aid of the party."
    val enrichedValue = longString.limitCharacters(10)
    assert(enrichedValue.size === 10)
  }

  it should "not limit strings shorter or equal to the limit" in {
    val shortString = "Now is the time"
    val enrichedValue = shortString.limitCharacters(shortString.length)
    assert(enrichedValue.size === shortString.length)
  }

  "reduceWhitespace" should "reduce two whitespaces to one whitespace" in {
    val originalValue = "foo  bar"
    val enrichedValue = originalValue.reduceWhitespace
    assert(enrichedValue === "foo bar")
  }

}
