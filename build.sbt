// Your sbt build file. Guides on how to write one can be found at
// http://www.scala-sbt.org/0.13/docs/index.html
name := "shc"

version := "0.1-1.6.0-SNAPSHOT"

organization := "zhzhan"

scalaVersion := "2.10.4"

sparkVersion := "1.6.0"

spName := "zhzhan/shc"

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion := sys.props.getOrElse("spark.testVersion", sparkVersion.value)

val testHadoopVersion = settingKey[String]("The version of Hadoop to test against.")

testHadoopVersion := sys.props.getOrElse("hadoop.testVersion", "2.2.0")

spAppendScalaVersion := true

spIncludeMaven := false 

spIgnoreProvided := true

// Don't forget to set the version
version := "0.0.1"

// All Spark Packages need a license
licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials") // A file containing credentials

unmanagedSourceDirectories in Compile <<= baseDirectory(base =>
  (base / "src" / "main" / "scala") :: Nil
)


// Add Spark components this package depends on, e.g, "mllib", ....
sparkComponents ++= Seq("core", "catalyst", "sql", "mllib")

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % "2.10.4", 
  "org.apache.hbase" % "hbase-server" % "1.1.0" exclude("asm", "asm") exclude("org.jboss.netty", "netty") exclude("io.netty", "netty-all") exclude("io.netty", "netty") exclude("commons-logging", "commons-logging") exclude("org.jruby","jruby-complete"),
  "org.apache.hbase" % "hbase-common" % "1.1.0" exclude("asm", "asm") exclude("org.jboss.netty", "netty") exclude("io.netty", "netty") exclude("commons-logging", "commons-logging") exclude("org.jruby","jruby-complete"),
//  "org.apache.hbase" % "hbase-testing-util" % "1.2.0" % "test",
  "org.apache.avro" % "avro" % "1.7.6", 
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)

pomExtra :=
    <scm>
      <url>git@github.com:zhzhan/shc</url>
      <connection>scm:git:git@github.com:zhzhan/shc.git</connection>
    </scm>
    <developers>
      <developer>
        <id>zhzhan</id>
        <name>Zhan Zhang</name>
      </developer>
    </developers>

// uncomment and change the value below to change the directory where your zip artifact will be created
// spDistDirectory := target.value

// add any Spark Package dependencies using spDependencies.
// e.g. spDependencies += "databricks/spark-avro:0.1"
