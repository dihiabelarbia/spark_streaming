package scala

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.nio.file.{Files, Paths, StandardCopyOption}

object _00_CsvBatchProcessingApp {
  def main(args: Array[String]): Unit = {
    System.setProperty("log4j.configurationFile", "src/main/resources/log4j2.properties")
    // Initialisation du SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("CsvBatchProcessingApp")
      .master("local[*]")
      .getOrCreate()

    // Lire le fichier CSV en DataFrame
    val inputFilePath = "csv/test_data_corrected.csv"
    val outputDirectory = "csvOutPut"

    val csvDF = spark.read.option("header", "true").csv(inputFilePath)

    // Sélectionner les colonnes requises et convertir la colonne _c0
    val formattedDF = csvDF
      .select(
        col("_c0"),
        col("signal_std").cast("double"),
        col("signal_rad").cast("double"),
        col("pluie").cast("int")
      )
      .withColumn("_c0", date_format(to_utc_timestamp(col("_c0"), "UTC"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))

    // Convertir le DataFrame en une séquence de lignes
    val lines = formattedDF.collect().toSeq
    val batchSize = 1000
    val numBatches = (lines.length + batchSize - 1) / batchSize  // Nombre total de lots

    // Définir le schéma basé sur formattedDF
    val schema = formattedDF.schema

    // Traiter les données par lots et écrire dans un fichier CSV
    for (i <- 0 until numBatches) {
      val start = i * batchSize
      val end = math.min((i + 1) * batchSize, lines.length)
      val batch = lines.slice(start, end)

      // Créer un DataFrame pour le lot actuel en utilisant le schéma
      val batchDF = spark.createDataFrame(spark.sparkContext.parallelize(batch), schema)

      // Coalesce to 1 partition to write a single CSV file
      val tempOutputPath = s"$outputDirectory/batch_temp_$i"
      batchDF.coalesce(1).write
        .option("header", "true")
        .mode(SaveMode.Overwrite)
        .csv(tempOutputPath)

      // Renommer le fichier CSV écrit en un nom spécifique
      val tempPath = Paths.get(tempOutputPath)
      val outputPath = Paths.get(s"$outputDirectory/batch_$i.csv")

      // Trouver le fichier écrit par Spark
      val tempFile = Files.list(tempPath).filter(path => path.toString.endsWith(".csv")).findFirst().get()

      // Déplacer le fichier
      Files.move(tempFile, outputPath, StandardCopyOption.REPLACE_EXISTING)

      // Supprimer le répertoire temporaire
      Files.walk(tempPath)
        .sorted(java.util.Comparator.reverseOrder())
        .map(_.toFile)
        .forEach(_.delete())

      // Pause d'une seconde
      Thread.sleep(15000)
    }

    // Arrêter SparkSession
    spark.stop()
  }
}