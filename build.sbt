
// The simplest possible sbt build file is just one line:

scalaVersion := "2.12.18"

name := "Takehome"
organization := "ch.epfl.scala"
version := "1.0"

//libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "2.3.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "org.apache.spark" %% "spark-mllib" % "3.5.0",
  "org.scalatest" %% "scalatest" % "3.2.10" % "test",
  "org.plotly-scala" %% "plotly-almond" % "0.8.2",
  "com.typesafe" % "config" % "1.4.1"
)