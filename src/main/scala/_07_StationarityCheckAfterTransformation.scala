import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object _07_StationarityCheckAfterTransformation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("StationarityCheck")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Charger les données depuis le fichier CSV
    val filePath = "csvInter/differenced_signals"
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(filePath)

    // Afficher les premières lignes du fichier CSV
    println("Contenu du fichier differenced_signals.csv :")
    df.show(10)

    // Ajouter une colonne timestamp
    val dfWithTimestamp = df.withColumn("timestamp", unix_timestamp(col("_c0"), "yyyy-MM-dd HH:mm:ss").cast("timestamp"))

    // Créer une fenêtre pour les opérations de décalage
    val windowSpec = Window.orderBy("timestamp")

    // Afficher les résultats
    dfWithTimestamp.select("timestamp", "signal_std_diff", "signal_rad_diff").show(10)

    // Calcul des statistiques segmentées par semaine
    val segmentedStats = dfWithTimestamp.groupBy(window($"timestamp", "1 week"))
      .agg(
        avg("signal_std_diff").alias("mean_signal_std_diff"),
        stddev("signal_std_diff").alias("stddev_signal_std_diff"),
        avg("signal_rad_diff").alias("mean_signal_rad_diff"),
        stddev("signal_rad_diff").alias("stddev_signal_rad_diff")
      )
    segmentedStats.show()

    // Vérification de la stationnarité
    val segmentedStdDev = segmentedStats.select("stddev_signal_std_diff", "stddev_signal_rad_diff").collect()
    val segmentedMeans = segmentedStats.select("mean_signal_std_diff", "mean_signal_rad_diff").collect()

    val stddevVaries = segmentedStdDev.map(row => (row.getDouble(0), row.getDouble(1))).distinct.length > 1
    val meanVaries = segmentedMeans.map(row => (row.getDouble(0), row.getDouble(1))).distinct.length > 1

    if (stddevVaries || meanVaries) {
      println("Les données ne sont pas stationnaires après transformation car la moyenne ou l'écart-type varient avec le temps.")
    } else {
      println("Les données sont stationnaires après transformation car la moyenne et l'écart-type sont constantes avec le temps.")
    }

    // Fermer la session Spark
    spark.stop()
  }
}
