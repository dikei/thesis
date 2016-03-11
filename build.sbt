name := "thesis"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "com.google.guava" % "guava" % "19.0",
  "org.apache.spark" % "spark-core_2.10" % "1.6.1" % "provided",
  "org.apache.spark" % "spark-mllib_2.10" % "1.6.1" % "provided",
  "org.apache.spark" % "spark-graphx_2.10" % "1.6.1" % "provided",
  "net.sf.supercsv" % "super-csv" % "2.4.0"
)

assemblyJarName in assembly := s"${name.value}-assembly-${version.value}.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)