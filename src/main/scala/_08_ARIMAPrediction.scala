import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.knowm.xchart.style.markers.SeriesMarkers
import org.knowm.xchart.{BitmapEncoder, XYChartBuilder}

object _08_ARIMAPrediction {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ARIMAPrediction")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Charger les données depuis le fichier CSV
    val filePath = "csvInter/differenced_signals"
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(filePath)

    // Ajouter une colonne timestamp
    val dfWithTimestamp = df.withColumn("timestamp", unix_timestamp(col("_c0"), "yyyy-MM-dd HH:mm:ss").cast("timestamp"))

    // Filtrer les valeurs nulles
    val dfFiltered = dfWithTimestamp.filter($"signal_std_diff".isNotNull && $"signal_rad_diff".isNotNull)

    // Convertir les colonnes en type double
    val dfWithDoubles = dfFiltered
      .withColumn("signal_std_diff", col("signal_std_diff").cast("double"))
      .withColumn("signal_rad_diff", col("signal_rad_diff").cast("double"))

    // Créer un assembleur de vecteurs pour les caractéristiques
    val assembler = new VectorAssembler()
      .setInputCols(Array("signal_std_diff"))
      .setOutputCol("features")

    // Transformer les données pour inclure les caractéristiques assemblées
    val assembled = assembler.transform(dfWithDoubles)

    // Séparer les données en ensembles d'entraînement et de test
    val Array(trainingData, testData) = assembled.randomSplit(Array(0.8, 0.2))

    // Vérifier la taille des ensembles d'entraînement et de test
    println(s"Training data count: ${trainingData.count()}")
    println(s"Test data count: ${testData.count()}")

    if (trainingData.isEmpty) {
      println("Training dataset is empty. Exiting.")
      spark.stop()
      return
    }

    // Entraîner le modèle de régression linéaire
    val lr = new LinearRegression()
      .setLabelCol("signal_rad_diff")
      .setFeaturesCol("features")
    val lrModel = lr.fit(trainingData)

    // Faire des prédictions
    val predictions = lrModel.transform(testData)

    // Afficher les résultats des prédictions
    predictions.select("timestamp", "signal_rad_diff", "prediction").show()

    // Convertir les résultats en listes pour le graphique
    val timeStamps = predictions.select("timestamp").as[Long].collect().toList.map(_.toDouble)
    val actuals = predictions.select("signal_rad_diff").as[Double].collect().toList
    val preds = predictions.select("prediction").as[Double].collect().toList

    // Créer le graphique
    val chart = new XYChartBuilder().width(800).height(600).title("ARIMA Predictions").xAxisTitle("Time").yAxisTitle("Value").build()

    // Ajouter les séries de données
    val series1 = chart.addSeries("Actual", timeStamps.toArray, actuals.toArray)
    series1.setMarker(SeriesMarkers.NONE)

    val series2 = chart.addSeries("Prediction", timeStamps.toArray, preds.toArray)
    series2.setMarker(SeriesMarkers.NONE)

    // Afficher le graphique
    BitmapEncoder.saveBitmap(chart, "predictions.png", BitmapEncoder.BitmapFormat.PNG)

    spark.stop()
  }
}
