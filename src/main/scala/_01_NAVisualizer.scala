import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.jfree.chart.axis.DateAxis
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer
import org.jfree.chart.{ChartFactory, ChartPanel, JFreeChart}
import org.jfree.data.xy.{XYSeries, XYSeriesCollection}
import org.jfree.ui.{ApplicationFrame, RefineryUtilities}

import java.awt.geom.Ellipse2D
import java.awt.{Color, Shape}
import java.text.SimpleDateFormat

object _01_NAVisualizer {
  def main(args: Array[String]): Unit = {
    // Initialiser SparkSession
    val spark = SparkSession.builder()
      .appName("RainDetection")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Charger les données depuis un fichier CSV
    val filePath2 = "csv/data_part2.csv"
    val df2 = spark.read.option("header", "true").option("inferSchema", "true").csv(filePath2)

    // Sélectionner les colonnes d'intérêt
    val selectedColumnsDf2 = df2.select("_c0", "signal_std", "signal_rad", "pluie")

    // Convertir la colonne de date en timestamp
    val dfWithTimestamp = selectedColumnsDf2.withColumn("timestamp", unix_timestamp(col("_c0")).cast("timestamp"))

    // Trouver la première date avec des valeurs non-nulles pour signal_std et signal_rad
    val firstNonNaTimestamp = dfWithTimestamp
      .filter($"signal_std".isNotNull && $"signal_rad".isNotNull)
      .orderBy("timestamp")
      .select("timestamp")
      .first()
      .getAs[java.sql.Timestamp]("timestamp")

    // Filtrer les lignes pour supprimer les NA jusqu'à la première valeur non-nulle
    val cleanedDf = dfWithTimestamp.filter($"timestamp" >= lit(firstNonNaTimestamp))

    val naRowsDf2 = cleanedDf.filter(
      col("_c0").isNull || col("signal_std").isNull || col("signal_rad").isNull || col("pluie").isNull
    )

    println("Lignes contenant des valeurs NA :")
    naRowsDf2.show()

    // Compter le nombre de lignes vides (sans prélevement)
    val emptyLinesCount2 = naRowsDf2.count()
    val shapeOfdf2 = cleanedDf.count()

    println(s"Number of lines with NA values: $emptyLinesCount2 / $shapeOfdf2 ")

    // Collecter les résultats pour le graphique
    val allRowsList2 = cleanedDf.collect()

    // Créer les séries pour JFreeChart
    val seriesSignalStd = new XYSeries("signal_std")
    val seriesSignalRad = new XYSeries("signal_rad")
    val seriesNA = new XYSeries("NA")

    allRowsList2.foreach { row =>
      val timestamp = row.getAs[java.sql.Timestamp]("timestamp").getTime.toDouble
      if (row.anyNull) {
        seriesNA.add(timestamp, -8)
      } else {
        seriesSignalStd.add(timestamp, row.getAs[Double]("signal_std"))
        seriesSignalRad.add(timestamp, row.getAs[Double]("signal_rad"))
      }
    }

    // Créer le dataset
    val dataset = new XYSeriesCollection()
    dataset.addSeries(seriesSignalStd)
    dataset.addSeries(seriesSignalRad)
    dataset.addSeries(seriesNA)

    // Créer le graphique
    val chart: JFreeChart = ChartFactory.createScatterPlot(
      "Valeurs des signaux par date",
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

    renderer.setSeriesPaint(0, Color.GREEN) // signal_std en vert
    renderer.setSeriesPaint(1, Color.BLUE)  // signal_rad en bleu
    renderer.setSeriesPaint(2, Color.RED)   // NA en rouge
    plot.setRenderer(renderer)

    // Configurer l'axe des x pour afficher les mois
    val dateAxis = new DateAxis("Date")
    dateAxis.setDateFormatOverride(new SimpleDateFormat("MMM yyyy"))
    plot.setDomainAxis(dateAxis)

    // Afficher le graphique dans une fenêtre
    val chartFrame = new ApplicationFrame("Graphique des Signaux")
    val chartPanel = new ChartPanel(chart)
    chartPanel.setPreferredSize(new java.awt.Dimension(800, 600))
    chartFrame.setContentPane(chartPanel)
    chartFrame.pack()
    RefineryUtilities.centerFrameOnScreen(chartFrame)
    chartFrame.setVisible(true)

    // Sauvegarder le cleanedDfs dans un fichier CSV
    cleanedDf.write.option("header", "true").csv("csvInter/cleanedDf")


    // Fermer la session Spark
    spark.stop()
  }
}
