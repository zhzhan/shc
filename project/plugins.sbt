// You may use this file to add plugin dependencies for sbt.
resolvers ++= Seq(
  "Sonatype OSS Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/",
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/",
  "Maven Repository" at "https://repo1.maven.org/maven2",
  "Apache Repository" at "https://repository.apache.org/content/repositories/releases"
)

addSbtPlugin("org.spark-packages" %% "sbt-spark-package" % "0.2.3")
