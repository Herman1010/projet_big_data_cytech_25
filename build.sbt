ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val sparkV  = "3.5.4"
lazy val hadoopV = "3.3.4"   // IMPORTANT: aligné avec Spark

lazy val root = (project in file("."))
  .settings(
    name := "new_projet_big_data",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkV,
      "org.apache.spark" %% "spark-sql"  % sparkV,

      // S3A / MinIO : version Hadoop alignée
      "org.apache.hadoop" % "hadoop-aws" % hadoopV,
      "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.262",

      "org.postgresql" % "postgresql" % "42.7.3"
    ),

    // Force tout Hadoop à la même version (évite le mélange)
    dependencyOverrides ++= Seq(
      "org.apache.hadoop" % "hadoop-common"         % hadoopV,
      "org.apache.hadoop" % "hadoop-client-api"     % hadoopV,
      "org.apache.hadoop" % "hadoop-client-runtime" % hadoopV
    )
  )
