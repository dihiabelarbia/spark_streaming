import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.jfree.chart.axis.DateAxis
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer
import org.jfree.chart.{ChartFactory, ChartPanel, JFreeChart}
import org.jfree.data.xy.{XYSeries, XYSeriesCollection}
import org.jfree.ui.{ApplicationFrame, RefineryUtilities}

import java.awt.geom.Ellipse2D
import java.awt.{Color, Shape}
import java.text.SimpleDateFormat

object _04_OutlierInterpolationVisualization {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("OutlierInterpolationVisualization")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val filePath = "csvInter/nan_interpolated_signals"
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(filePath)

    val selectedColumnsDf = df.select("_c0", "signal_std_interpolated", "signal_rad_interpolated", "pluie")
    val dfWithTimestamp = selectedColumnsDf.withColumn("timestamp", unix_timestamp(col("_c0"), "yyyy-MM-dd HH:mm:ss").cast("timestamp"))

    // Ajouter une colonne d'index pour l'ordonnancement
    val windowSpec = Window.orderBy("timestamp")
    var dfWithId = dfWithTimestamp.withColumn("id", monotonically_increasing_id())

    // Calculer les statistiques pour détecter les outliers
    val statsStd = dfWithId.agg(mean("signal_std_interpolated").alias("mean"), stddev("signal_std_interpolated").alias("stddev")).first()
    val meanStd = statsStd.getDouble(0)
    val stddevStd = statsStd.getDouble(1)
    val threshold = 3.0

    val statsRad = dfWithId.agg(mean("signal_rad_interpolated").alias("mean"), stddev("signal_rad_interpolated").alias("stddev")).first()
    val meanRad = statsRad.getDouble(0)
    val stddevRad = statsRad.getDouble(1)

    // Ajouter une colonne pour indiquer les outliers
    val dfWithOutliers = dfWithId.withColumn("is_outlier",
      abs(col("signal_std_interpolated") - meanStd) > threshold * stddevStd || abs(col("signal_rad_interpolated") - meanRad) > threshold * stddevRad)

    // Interpoler les valeurs de signal_std et signal_rad
    val interpolatedDf = dfWithOutliers.withColumn("signal_std_final",
      when(col("is_outlier"), (last("signal_std_interpolated", ignoreNulls = true).over(windowSpec) + first("signal_std_interpolated", ignoreNulls = true).over(windowSpec)) / 2)
        .otherwise(col("signal_std_interpolated"))
    ).withColumn("signal_rad_final",
      when(col("is_outlier"), (last("signal_rad_interpolated", ignoreNulls = true).over(windowSpec) + first("signal_rad_interpolated", ignoreNulls = true).over(windowSpec)) / 2)
        .otherwise(col("signal_rad_interpolated"))
    )

    // Collecter les résultats pour le graphique
    val allRowsList = interpolatedDf.collect()

    // Créer les séries pour JFreeChart
    val seriesSignalStd = new XYSeries("signal_std_final")
    val seriesSignalRad = new XYSeries("signal_rad_final")
    val seriesOutliers = new XYSeries("outliers")
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    allRowsList.foreach { row =>
      val timestamp = row.getAs[java.sql.Timestamp]("timestamp").getTime.toDouble
      if (row.getAs[Boolean]("is_outlier")) {
        seriesOutliers.add(timestamp, row.getAs[Double]("signal_std_interpolated"))
      } else {
        seriesSignalStd.add(timestamp, row.getAs[Double]("signal_std_final"))
        seriesSignalRad.add(timestamp, row.getAs[Double]("signal_rad_final"))
      }
    }

    // Créer le dataset
    val dataset = new XYSeriesCollection()
    dataset.addSeries(seriesSignalStd)
    dataset.addSeries(seriesSignalRad)
    dataset.addSeries(seriesOutliers)

    // Créer le graphique
    val chart: JFreeChart = ChartFactory.createScatterPlot(
      "Valeurs des signaux après traitement des outliers",
      "Date",
      "Valeur du signal",
      dataset
    )

    // Configurer les couleurs des séries
    val plot = chart.getXYPlot
    val renderer = new XYLineAndShapeRenderer(false, true) // false pour ne pas dessiner les lignes, true pour dessiner les points

    // Définir des petits points ronds
    val shape: Shape = new Ellipse2D.Double(-2.0, -2.0, 4.0, 4.0)
    renderer.setSeriesShape(0, shape)
    renderer.setSeriesShape(1, shape)
    renderer.setSeriesShape(2, shape)

    renderer.setSeriesPaint(0, Color.GREEN) // signal_std_final en vert
    renderer.setSeriesPaint(1, Color.BLUE)  // signal_rad_final en bleu
    renderer.setSeriesPaint(2, Color.BLACK) // outliers en noir
    plot.setRenderer(renderer)

    // Configurer l'axe des x pour afficher les dates
    val dateAxis = new DateAxis("Date")
    dateAxis.setDateFormatOverride(new SimpleDateFormat("MMM yyyy"))
    plot.setDomainAxis(dateAxis)

    // Afficher le graphique dans une fenêtre
    val chartFrame = new ApplicationFrame("Graphique des Signaux après Traitement des Outliers")
    val chartPanel = new ChartPanel(chart)
    chartPanel.setPreferredSize(new java.awt.Dimension(800, 600))
    chartFrame.setContentPane(chartPanel)
    chartFrame.pack()
    RefineryUtilities.centerFrameOnScreen(chartFrame)
    chartFrame.setVisible(true)

    // Sauvegarder les résultats dans un fichier CSV
    interpolatedDf.select("_c0", "signal_std_final", "signal_rad_final", "pluie")
      .write.option("header", "true").csv("csvInter/interpolated_signals_no_outliers")

    // Fermer la session Spark
    spark.stop()
  }
}
