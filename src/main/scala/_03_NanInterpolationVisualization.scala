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

object _03_NanInterpolationVisualization {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("InterpolationVisualization")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val filePath = "csvInter/cleanedDf"
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(filePath)

    val selectedColumnsDf = df.select("_c0", "signal_std", "signal_rad", "pluie")
    val dfWithTimestamp = selectedColumnsDf.withColumn("timestamp", unix_timestamp(col("_c0"), "yyyy-MM-dd HH:mm:ss").cast("timestamp"))

    // Ajouter une colonne d'index pour l'ordonnancement
    val windowSpec = Window.orderBy("timestamp")
    val dfWithId = dfWithTimestamp.withColumn("id", monotonically_increasing_id())

    // Interpoler les valeurs de signal_std
    val interpolatedSignalStdDf = dfWithId.withColumn("signal_std_interpolated",
      when(col("signal_std").isNotNull, col("signal_std"))
        .otherwise(
          (last("signal_std", ignoreNulls = true).over(windowSpec) + first("signal_std", ignoreNulls = true).over(windowSpec)) / 2
        )
    )

    // Interpoler les valeurs de signal_rad
    val interpolatedSignalRadDf = interpolatedSignalStdDf.withColumn("signal_rad_interpolated",
      when(col("signal_rad").isNotNull, col("signal_rad"))
        .otherwise(
          (last("signal_rad", ignoreNulls = true).over(windowSpec) + first("signal_rad", ignoreNulls = true).over(windowSpec)) / 2
        )
    )

    // Collecter les résultats pour le graphique
    val allRowsList = interpolatedSignalRadDf.collect()

    // Créer les séries pour JFreeChart
    val seriesSignalStd = new XYSeries("signal_std_interpolated")
    val seriesSignalRad = new XYSeries("signal_rad_interpolated")
    val seriesInterpolated = new XYSeries("interpolated_points")
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    allRowsList.foreach { row =>
      val timestamp = row.getAs[java.sql.Timestamp]("timestamp").getTime.toDouble
      if (row.getAs[Double]("signal_std") == row.getAs[Double]("signal_std_interpolated")) {
        seriesSignalStd.add(timestamp, row.getAs[Double]("signal_std_interpolated"))
      } else {
        seriesInterpolated.add(timestamp, row.getAs[Double]("signal_std_interpolated"))
      }
      if (row.getAs[Double]("signal_rad") == row.getAs[Double]("signal_rad_interpolated")) {
        seriesSignalRad.add(timestamp, row.getAs[Double]("signal_rad_interpolated"))
      } else {
        seriesInterpolated.add(timestamp, row.getAs[Double]("signal_rad_interpolated"))
      }
    }

    // Créer le dataset
    val dataset = new XYSeriesCollection()
    dataset.addSeries(seriesSignalStd)
    dataset.addSeries(seriesSignalRad)
    dataset.addSeries(seriesInterpolated)

    // Créer le graphique
    val chart: JFreeChart = ChartFactory.createScatterPlot(
      "Valeurs des signaux après interpolation",
      "Date",
      "Valeur du signal",
      dataset
    )

    // Configurer les couleurs des séries
    val plot = chart.getXYPlot
    val renderer = new XYLineAndShapeRenderer(false, true) // false pour ne pas dessiner les lignes

    // Définir des petits points ronds
    val shape: Shape = new Ellipse2D.Double(-1.0, -1.0, 2.0, 2.0)
    renderer.setSeriesShape(0, shape)
    renderer.setSeriesShape(1, shape)
    renderer.setSeriesShape(2, shape)

    renderer.setSeriesPaint(0, Color.GREEN) // signal_std_interpolated en vert
    renderer.setSeriesPaint(1, Color.BLUE)  // signal_rad_interpolated en bleu
    renderer.setSeriesPaint(2, Color.RED)   // interpolated_points en rouge
    plot.setRenderer(renderer)

    // Configurer l'axe des x pour afficher les dates
    val dateAxis = new DateAxis("Date")
    dateAxis.setDateFormatOverride(new SimpleDateFormat("MMM yyyy"))
    plot.setDomainAxis(dateAxis)

    // Afficher le graphique dans une fenêtre
    val chartFrame = new ApplicationFrame("Graphique des Signaux après Interpolation")
    val chartPanel = new ChartPanel(chart)
    chartPanel.setPreferredSize(new java.awt.Dimension(800, 600))
    chartFrame.setContentPane(chartPanel)
    chartFrame.pack()
    RefineryUtilities.centerFrameOnScreen(chartFrame)
    chartFrame.setVisible(true)

    interpolatedSignalRadDf.select("_c0", "signal_std_interpolated", "signal_rad_interpolated", "pluie").show()

    // Sauvegarder les résultats dans un fichier CSV
    interpolatedSignalRadDf.select("_c0", "signal_std_interpolated", "signal_rad_interpolated", "pluie")
      .write.option("header", "true").csv("csvInter/nan_interpolated_signals")

    // Fermer la session Spark
    spark.stop()
  }
}
