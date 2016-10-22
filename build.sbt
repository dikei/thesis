name := "thesis"

version := "1.0"

scalaVersion := "2.10.6"

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")
scalacOptions += "-target:jvm-1.7"

libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-math3" % "3.6.1",
  "com.google.guava" % "guava" % "19.0",
  "org.apache.spark" % "spark-core_2.10" % "1.6.2-SNAPSHOT" % "provided",
  "org.apache.spark" % "spark-mllib_2.10" % "1.6.2-SNAPSHOT" % "provided",
  "org.apache.spark" % "spark-graphx_2.10" % "1.6.2-SNAPSHOT" % "provided",
  "org.json4s" %% "json4s-native" % "3.3.0",
  "org.json4s" %% "json4s-ext" % "3.3.0",
  "net.sf.supercsv" % "super-csv" % "2.4.0",
  "org.jfree" % "jfreechart" % "1.0.19",
  "org.projectlombok" % "lombok" % "1.16.10" % "provided",
  "com.itextpdf" % "itextpdf" % "5.5.9"
)

assemblyJarName in assembly := s"${name.value}-assembly-${version.value}.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

fork := true
compileOrder := CompileOrder.JavaThenScala