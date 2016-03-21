// Your sbt build file. Guides on how to write one can be found at
// http://www.scala-sbt.org/0.13/docs/index.html
name := "spark-hbase-connector"

organization := "com.hortonworks"

scalaVersion := "2.10.4"

sparkVersion := "1.6.0"

spName := "Hortonworks/hbase-spark-connector"

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion := sys.props.getOrElse("spark.testVersion", sparkVersion.value)

val testHadoopVersion = settingKey[String]("The version of Hadoop to test against.")

testHadoopVersion := sys.props.getOrElse("hadoop.testVersion", "2.2.0")

spAppendScalaVersion := true

spIncludeMaven := true

spIgnoreProvided := true

// Don't forget to set the version
version := "0.0.1"

// All Spark Packages need a license
licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))


// Add Spark components this package depends on, e.g, "mllib", ....
sparkComponents ++= Seq("core", "catalyst", "sql", "mllib")

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % "2.10.4", 
  "org.apache.hbase" % "hbase-server" % "1.1.1" exclude("asm", "asm") exclude("org.jboss.netty", "netty") exclude("io.netty", "netty") exclude("commons-logging", "commons-logging") exclude("org.jruby","jruby-complete"),
//  "org.apache.hbase" % "hbase-testing-util" % "1.1.1"  % "test",
  "org.apache.avro" % "avro" % "1.7.6", 
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)

// uncomment and change the value below to change the directory where your zip artifact will be created
// spDistDirectory := target.value

// add any Spark Package dependencies using spDependencies.
// e.g. spDependencies += "databricks/spark-avro:0.1"
