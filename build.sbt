
name := "ingestion3"
organization := "dpla"
version := "0.0.1"
scalaVersion := "2.12.15"

parallelExecution in Test := false

// https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-release-app-versions-6.x.html
val HADOOP_VERSION = "3.2.1" // For emr-6.5.0
val AWS_SDK_VERSION = "1.12.31" // For emr-6.5.0

// TODO ...
// Spark 3.2.1 uses jackson 2.12
// Spark 3.1.2 uses jackson 2.10 but this gets evicted by jackson 2.12 that comes from ... somewhere ...
// Spark 3.1.2 is needed for AWS EMR 6.5.0
// TODO figure out where jackson 2.12 is coming from so Spark 3.1.2 can be used on EMR
val SPARK_VERSION = "3.2.1" // // For emr-6.5.0

assembly / assemblyMergeStrategy := {
  case "META-INF/MANIFEST.MF" => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % SPARK_VERSION exclude("org.scalatest", "scalatest_2.11"),
  "org.apache.spark" %% "spark-sql" % SPARK_VERSION exclude("org.scalatest", "scalatest_2.11"),
  "org.apache.spark" %% "spark-mllib" % SPARK_VERSION exclude("org.scalatest", "scalatest_2.11"),
  "org.apache.spark" %% "spark-avro" % SPARK_VERSION exclude("org.scalatest", "scalatest_2.11"),

  "org.apache.hadoop" % "hadoop-aws" % HADOOP_VERSION,
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % HADOOP_VERSION,

  "com.amazonaws" % "aws-java-sdk" % AWS_SDK_VERSION,

  "org.apache.ant" % "ant" % "1.10.1",

  // JSON parsers
  // 3.7.0-M2 pulls in the correct version of com.fasterxml.jackson
  //  > Caused by: com.fasterxml.jackson.databind.JsonMappingException: Scala module 2.10.0 requires Jackson Databind version >= 2.10.0 and < 2.11.0
  // "org.json4s" %% "json4s-core" % "3.7.0-M2", // % "provided",
  "org.json4s" %% "json4s-native" % "3.7.0-M5" % "provided",
  // "org.json4s" %% "json4s-jackson" % "3.7.0-M2", // % "provided",

  // Enricher dependencies
  "org.jsoup" % "jsoup" % "1.10.2", // Used for StringNormalization

  // HTTPs
  "org.apache.httpcomponents" % "httpclient" % "4.5.2", // CdlHarvester depends
  "org.apache.httpcomponents" % "fluent-hc" % "4.5.2", // CdlHarvester depends
  "org.scalaj" %% "scalaj-http" % "2.4.2",
  "org.scalaj" % "scalaj-http_2.12" % "2.4.2",
  "com.squareup.okhttp3" % "okhttp" % "4.9.3",

  // What do we say to the God of LD? Not today.
  "org.eclipse.rdf4j" % "rdf4j-model" % "2.2",
  "org.eclipse.rdf4j" % "rdf4j-rio-api" % "2.2",
  "org.eclipse.rdf4j" % "rdf4j-rio-turtle" % "2.2",
  "org.eclipse.rdf4j" % "rdf4j" % "2.2",
  "org.eclipse.rdf4j" % "rdf4j-model" % "2.2",
  "org.eclipse.rdf4j" % "rdf4j-rio-api" % "2.2",
  "org.eclipse.rdf4j" % "rdf4j-rio-turtle" % "2.2",

  // Configs
  "org.rogach" %% "scallop" % "3.0.3",
  "com.typesafe" % "config" % "1.3.1",

  // Tests
  "org.scalamock" %% "scalamock" % "4.0.0",
  // "com.holdenkarau" %% "spark-testing-base" % "3.1.2_1.1.1", % "test", Is this still needed? 
  "org.scalatest" %% "scalatest" % "3.0.1",

  // IDK what this is used for in the project
  "com.opencsv" % "opencsv" % "3.10",

  // NLP
  "databricks" % "spark-corenlp" % "0.3.1-s_2.11",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.9.1" classifier "models",

  // ElasticSearch
  "org.elasticsearch" %% "elasticsearch-spark-20" % "7.17.3" // eleanor
)

 resolvers += "SparkPackages" at "https://repos.spark-packages.org/"
