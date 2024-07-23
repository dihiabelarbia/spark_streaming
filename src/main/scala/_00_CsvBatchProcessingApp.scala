import org.apache.spark.sql.{SaveMode, SparkSession}

object CsvBatchProcessingApp {
  def main(args: Array[String]): Unit = {
    System.setProperty("log4j.configurationFile", "src/main/resources/log4j2.properties")
    // Initialisation du SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("CsvBatchProcessingApp")
      .master("local[*]")
      .getOrCreate()

    // Lire le fichier CSV en DataFrame
    val inputFilePath = "csv/test_data_corrected.csv"
    val outputFilePath = "csvOutPut"

    val csvDF = spark.read.option("header", "true").csv(inputFilePath)

    // Convertir le DataFrame en une séquence de lignes
    val lines = csvDF.collect().toSeq
    val batchSize = 10000
    val numBatches = (lines.length + batchSize - 1) / batchSize  // Nombre total de lots

    // Définir le schéma basé sur csvDF
    val schema = csvDF.schema

    // Traiter les données par lots et écrire dans un fichier CSV
    for (i <- 0 until numBatches) {
      val start = i * batchSize
      val end = math.min((i + 1) * batchSize, lines.length)
      val batch = lines.slice(start, end)

      // Créer un DataFrame pour le lot actuel en utilisant le schéma
      val batchDF = spark.createDataFrame(spark.sparkContext.parallelize(batch), schema)

      // Écrire le lot dans un fichier CSV
      val batchOutputPath = s"$outputFilePath/batch_$i.csv"
      batchDF.coalesce(1).write
        .option("header", "true")
        .mode(SaveMode.Append)
        .csv(batchOutputPath)

      // Pause d'une seconde
      Thread.sleep(1000)
    }

    // Arrêter SparkSession
    spark.stop()
  }
}
