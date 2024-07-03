import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object _05_StationarityCheck {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("StationarityCheck")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val filePath = "csvInter/nan_interpolated_signals"
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(filePath)

    val dfWithTimestamp = df.withColumn("timestamp", unix_timestamp(col("_c0"), "yyyy-MM-dd HH:mm:ss").cast("timestamp"))
    val windowSpec = Window.orderBy("timestamp")

    // Calcul des statistiques globales
    val globalStats = dfWithTimestamp.describe("signal_std_interpolated", "signal_rad_interpolated")
    globalStats.show()

    // Calcul des statistiques segmentées par semaine
    val segmentedStats = dfWithTimestamp.groupBy(window($"timestamp", "1 week"))
      .agg(
        avg("signal_std_interpolated").alias("mean_signal_std"),
        stddev("signal_std_interpolated").alias("stddev_signal_std"),
        avg("signal_rad_interpolated").alias("mean_signal_rad"),
        stddev("signal_rad_interpolated").alias("stddev_signal_rad")
      )
    segmentedStats.show()

    // Vérification de la stationnarité
    val segmentedStdDev = segmentedStats.select("stddev_signal_std", "stddev_signal_rad").collect()
    val segmentedMeans = segmentedStats.select("mean_signal_std", "mean_signal_rad").collect()

    val stddevVaries = segmentedStdDev.map(row => (row.getDouble(0), row.getDouble(1))).distinct.length > 1
    val meanVaries = segmentedMeans.map(row => (row.getDouble(0), row.getDouble(1))).distinct.length > 1

    if (stddevVaries || meanVaries) {
      println("Les données ne sont pas stationnaires car la moyenne ou l'écart-type varient avec le temps.")
    } else {
      println("Les données sont stationnaires car la moyenne et l'écart-type sont constantes avec le temps.")
    }

    // Fermer la session Spark
    spark.stop()
  }
}
