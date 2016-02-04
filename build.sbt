name := "spark-testing"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "com.google.guava" % "guava" % "19.0"
)

assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter {_.data.getName == "spark-assembly-1.6.0-hadoop2.2.0.jar"}
}