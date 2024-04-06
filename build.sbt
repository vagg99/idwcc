name := "idwcc"

version := "0.1"

scalaVersion := "2.12.12"

resolvers += "Spark Packages Repo" at "https://repos.spark-packages.org/"


libraryDependencies ++= Seq(
  "org.scalaz" %% "scalaz-core" % "7.2.30",
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.0.0",
  "org.apache.spark" %% "spark-mllib" % "3.0.0",
  "org.apache.spark" %% "spark-streaming" % "3.0.0",
  "graphframes" % "graphframes" % "0.8.1-spark3.0-s_2.12"
)
