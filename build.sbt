name := "CommentsAnalysis"

version := "1.0"

scalaVersion := "2.10.4"

resolvers ++= Seq(
  "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/")

libraryDependencies ++= Seq(
  // Spark
  "org.apache.spark" %% "spark-core" % "1.4.1"% "provided",
  "org.apache.spark" %% "spark-sql" % "1.4.1"% "provided",
  "org.apache.spark" %% "spark-hive" % "1.4.1" % "provided",
  "com.databricks" %% "spark-csv" %"1.2.0" ,
  "com.databricks" %% "spark-redshift" % "0.5.2",

  // Others
  "postgresql" % "postgresql" % "9.1-901.jdbc4",
  "com.amazon.redshift" % "jdbc4" % "1.1.7.1007" from "https://s3.amazonaws.com/redshift-downloads/drivers/RedshiftJDBC4-1.1.7.1007.jar",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.10.29",
  "com.typesafe" % "config" % "1.3.0",
  "org.jsoup" % "jsoup" % "1.8.3"
)

