ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "bdaa"
  )
// Spark Core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.2"

// Spark SQL
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.0"

libraryDependencies += "org.scala-lang.modules" %% "scala-swing" % "3.0.0"

libraryDependencies  ++= Seq(
  "org.scalanlp" %% "breeze" % "2.1.0",
  "org.scalanlp" %% "breeze-viz" % "2.1.0"
)