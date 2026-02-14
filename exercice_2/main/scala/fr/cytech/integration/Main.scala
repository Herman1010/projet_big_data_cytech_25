package fr.cytech.integration

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.DriverManager
import scala.io.Source


object Main {

  def main(args: Array[String]): Unit = {

    // ---- CONFIG ----
    val cleanBucket = "nyc-clean"
    val cleanFileName = "yellow_tripdata_2024-01_clean.parquet"

    val rawPath = "s3a://nyc-raw/yellow_tripdata_2024-01.parquet"
    val cleanPathS3 = s"s3a://$cleanBucket/$cleanFileName"

    val jdbcUrl  = "jdbc:postgresql://localhost:5432/nyc_taxi_dw"
    val jdbcUser = "dw_user"
    val jdbcPwd  = "Big_data"
    val jdbcTable = "nyc_taxi.t_taxi_jaune" // c'est la table que nous allons utiliser

    // ---- SPARK ----
    val spark = SparkSession.builder()
      .appName("nyc-taxi-simple-ingestion")
      .master("local[*]")
      .getOrCreate()

    val hconf = spark.sparkContext.hadoopConfiguration
    hconf.set("fs.s3a.endpoint", "http://localhost:9000")
    hconf.set("fs.s3a.access.key", "minio")
    hconf.set("fs.s3a.secret.key", "minio123")
    hconf.set("fs.s3a.path.style.access", "true")
    hconf.set("fs.s3a.connection.ssl.enabled", "false")

    spark.sparkContext.setLogLevel("WARN")

    // ---- Lecture du fichier parquet ----
    val df = spark.read.parquet(rawPath)

    println(s"Lignes lues : ${df.count()}")

    // 2. Nettoyage
    // ==========================
    val cleanDF = df
      .filter(col("trip_distance") > 0)
      .filter(col("fare_amount") > 0)
      .filter(col("total_amount") > 0)
      .filter(col("passenger_count") >= 0)
      .filter(col("tpep_pickup_datetime").isNotNull)
      .filter(col("tpep_dropoff_datetime").isNotNull)
      .withColumn(
        "trip_duration_min",
        (unix_timestamp(col("tpep_dropoff_datetime")) -
          unix_timestamp(col("tpep_pickup_datetime"))) / 60
      )
      .filter(col("trip_duration_min") > 0)

    println(s"Nombre de lignes après nettoyage : ${cleanDF.count()}")

    // Écriture parquet clean dans MinIO
    println(s"Écriture du parquet nettoyé dans MinIO : $cleanPathS3")
    cleanDF.write.mode(SaveMode.Overwrite).parquet(cleanPathS3)
    println("   -> Parquet clean écrit avec succès dans nyc-clean ")

    // Création des tables et schémas dans le DWH

    def runSqlFile(path: String, jdbcUrl: String, user: String, pwd: String): Unit = {
      val sql = Source.fromFile(path).mkString
      val conn = DriverManager.getConnection(jdbcUrl, user, pwd)
      try {
        conn.createStatement().execute(sql)
      } finally {
        conn.close()
      }
    }


    runSqlFile("/home/cytech/IdeaProjects/new_projet_big_data/exercice_3/creation.sql", jdbcUrl, jdbcUser, jdbcPwd)


    // étape 3
    // On insère les données néttoyées dans le DWH
    val dfToInsert = cleanDF.select(
      col("tpep_pickup_datetime").cast("timestamp"),
      col("tpep_dropoff_datetime").cast("timestamp"),
      col("passenger_count").cast("int"),
      col("trip_distance"),
      col("RatecodeID").cast("int"),
      col("store_and_fwd_flag").cast("string"),
      col("PULocationID").cast("int"),
      col("DOLocationID").cast("int"),
      col("payment_type").cast("int"),
      col("fare_amount"),
      col("extra"),
      col("mta_tax"),
      col("tip_amount"),
      col("tolls_amount"),
      col("improvement_surcharge"),
      col("total_amount"),
      col("congestion_surcharge"),
      col("airport_fee")
    )

    dfToInsert.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", jdbcTable)
      .option("user", jdbcUser)
      .option("password", jdbcPwd)
      .option("driver", "org.postgresql.Driver")
      .mode(SaveMode.Append)
      .save()

    println("Insertion PostgreSQL terminée ")


    // Insertion du fichier taxi_zone_lookup.csv upload dans minio
    val csvBucket = "nyc-raw"
    val csvFileName = "taxi_zone_lookup.csv"
    val csvPath = s"s3a://$csvBucket/$csvFileName"

    val zoneTable = "nyc_taxi.t_zone"

    println(s"Lecture CSV zones depuis MinIO : $csvPath")

    val dfZoneRaw = spark.read
      .option("header", "true")
      .option("sep", ",")
      .csv(csvPath)

    val dfZone = dfZoneRaw.select(
      col("LocationID").cast("int").as("locationid"),
      col("Borough").cast("string").as("borough"),
      col("Zone").cast("string").as("zone"),
      col("service_zone").cast("string").as("service_zone")
    ).dropDuplicates("locationid")

    println(s"Lignes zones : ${dfZone.count()}")
    dfZone.printSchema()

    println(s"Insertion zones dans PostgreSQL : $zoneTable")
    dfZone.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", zoneTable)
      .option("user", jdbcUser)
      .option("password", jdbcPwd)
      .option("driver", "org.postgresql.Driver")
      .mode(SaveMode.Append)
      .save()

    println("Insertion taxi_zone_lookup terminée")


    // --- CSV dimension temps ---
    val dimFileName = "v_dimension_temps.csv"
    val dimPath = s"s3a://$csvBucket/$dimFileName"

    val dimTable = "config.t_dimension_temps"

    val dfDimRaw = spark.read
      .option("header", "true")
      .option("sep", ",")
      .csv(dimPath)


    val dfDim = dfDimRaw.select(
      to_date(col("jour")).as("jour"),
      col("timespan_jour").cast("bigint").as("timespan_jour"),
      to_date(col("end_date_jour")).as("end_date_jour"),

      col("mois").cast("string").as("mois"),
      col("timespan_mois").cast("bigint").as("timespan_mois"),
      to_date(col("end_date_mois")).as("end_date_mois"),

      col("annee").cast("int").as("annee"),
      col("timespan_annee").cast("bigint").as("timespan_annee"),
      to_date(col("end_date_annee")).as("end_date_annee"),

      col("semaine").cast("string").as("semaine"),
      col("timespan_semaine").cast("bigint").as("timespan_semaine"),
      to_date(col("end_date_semaine")).as("end_date_semaine"),

      col("ferie").cast("int").as("ferie"),
      col("periode_hebdo").cast("string").as("periode_hebdo"),
      col("periode_trimestrielle").cast("string").as("periode_trimestrielle"),

      col("mois_num").cast("int").as("mois_num"),
      col("semaine_num").cast("int").as("semaine_num"),

      col("periode_mensuelle").cast("string").as("periode_mensuelle"),
      col("lib_periode_mois").cast("string").as("lib_periode_mois"),
      col("lib_periode_semaine").cast("string").as("lib_periode_semaine"),
      col("semaine_num_iso").cast("string").as("semaine_num_iso"),

      col("jourouvrable").cast("int").as("jourouvrable")
    ).dropDuplicates("jour")

    println(s"Lignes dimension  à insérer : ${dfDim.count()}")

    // Insertion directe
    dfDim.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", dimTable)
      .option("user", jdbcUser)
      .option("password", jdbcPwd)
      .option("driver", "org.postgresql.Driver")
      .mode(SaveMode.Append)
      .save()

    runSqlFile("/home/cytech/IdeaProjects/new_projet_big_data/exercice_3/insertion.sql", jdbcUrl, jdbcUser, jdbcPwd) // création d'une clé primaire


    println("Terminée")


    spark.stop()
  }


}

