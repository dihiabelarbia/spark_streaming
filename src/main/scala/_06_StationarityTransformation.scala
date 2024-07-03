import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object _06_StationarityTransformation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("StationarityTransformation")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Charger les données depuis le fichier CSV
    val filePath = "csvInter/nan_interpolated_signals"
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(filePath)

    // Ajouter une colonne timestamp
    val dfWithTimestamp = df.withColumn("timestamp", unix_timestamp(col("_c0"), "yyyy-MM-dd HH:mm:ss").cast("timestamp"))

    // Créer une fenêtre pour les opérations de décalage
    val windowSpec = Window.orderBy("timestamp")

    // Étape 1: Différenciation de premier ordre sur les signaux interpolés
    val dfDifferenced = dfWithTimestamp
      .withColumn("signal_std_diff", col("signal_std_interpolated") - lag("signal_std_interpolated", 1).over(windowSpec))
      .withColumn("signal_rad_diff", col("signal_rad_interpolated") - lag("signal_rad_interpolated", 1).over(windowSpec))
      .filter(col("signal_std_diff").isNotNull && col("signal_rad_diff").isNotNull)

    // Afficher les résultats
    dfDifferenced.select("timestamp", "signal_std_diff", "signal_rad_diff").show(10)

    dfDifferenced.select("_c0", "signal_std_diff", "signal_rad_diff", "pluie").show(10)

    // Sauvegarder les résultats dans un fichier CSV
    dfDifferenced.select("_c0", "signal_std_diff", "signal_rad_diff", "pluie")
      .write.option("header", "true").csv("csvInter/differenced_signals")

    // Fermer la session Spark
    spark.stop()
  }
}
