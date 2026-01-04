ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

resolvers ++= Seq(
  "Spark Packages" at "https://repos.spark-packages.org/"
)


lazy val root = (project in file("."))
  .settings(
    name := "Tr",
    libraryDependencies ++= Seq(
      // Spark Core
      "org.apache.spark" %% "spark-core" % "3.2.4",

      // Spark SQL
      "org.apache.spark" %% "spark-sql" % "3.2.4",

      // Spark GraphX
      "org.apache.spark" %% "spark-graphx" % "3.2.4",

      // GraphFrames
      "graphframes" % "graphframes" % "0.8.2-spark3.2-s_2.12",


// Logging
      "org.apache.logging.log4j" % "log4j-api"  % "2.23.1"
    )
  )
