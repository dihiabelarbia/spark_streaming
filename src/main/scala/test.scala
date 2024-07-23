import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types._
import org.knowm.xchart.{SwingWrapper, XYChartBuilder}
import org.knowm.xchart.style.markers.SeriesMarkers // Corrected import

import java.nio.file.{Files, Paths}
import scala.collection.mutable.ListBuffer
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.ZoneOffset

object test {
  def main(args: Array[String]): Unit = {
    System.setProperty("log4j.configurationFile", "src/main/resources/log4j2.properties")

    val spark = SparkSession.builder()
      .appName("StreamingJoinExample")
      .master("local[*]")
      .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
      .config("spark.sql.streaming.checkpointLocation", "checkpoint/") // Ajoutez un point de contrôle
      .config("spark.sql.codegen.wholeStage", "false")
      .config("spark.sql.codegen.aggregate.map.twolevel.enable", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    import spark.implicits._

    // Define the schema
    val schema = new StructType()
      .add("_c0", TimestampType, true)
      .add("signal_std", DoubleType, true)
      .add("signal_rad", DoubleType, true)
      .add("pluie", IntegerType, true)

    // Read the data stream
    val directoryPath = "csvOutPut"

    while (!Files.exists(Paths.get(directoryPath))) {
      println(s"Dossier $directoryPath n'existe pas encore. Attente...")
      Thread.sleep(1000) // Attendre 1 seconde avant de vérifier à nouveau
    }
    val inputDf = spark.readStream
      .schema(schema)
      .option("header", "true")
      .csv(directoryPath)
      .as("input")

    inputDf.printSchema() // Print schema to check if it's correct

    // Filter out rows where any value is null
    val filteredDf = inputDf.filter($"_c0".isNotNull && $"signal_std".isNotNull && $"signal_rad".isNotNull && $"pluie".isNotNull)

    // Print initial data for debugging
    val query1 = filteredDf.writeStream
      .outputMode(OutputMode.Append())
      .format("console")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    // Define tumbling window duration
    val windowDuration = "60 minutes"

    // Apply window function and watermark
    val windowedDf = filteredDf
      .withWatermark("_c0", "60 minutes")
      .groupBy(window($"_c0", windowDuration))
      .agg(
        collect_list(struct($"_c0", $"signal_std", $"signal_rad", $"pluie")).as("rows")
      )
      .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("rows")
      )

    // Print windowed data for debugging
    val query2 = windowedDf.writeStream
      .outputMode(OutputMode.Append())
      .format("console")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    // Create a new XChart chart
    val chart = new XYChartBuilder().width(800).height(600).title("Real-time Streaming Data").xAxisTitle("Time").yAxisTitle("Values").build()
    chart.addSeries("Signal STD", Array(0.0), Array(0.0)).setMarker(SeriesMarkers.NONE)
    chart.addSeries("Signal RAD", Array(0.0), Array(0.0)).setMarker(SeriesMarkers.NONE)
    chart.addSeries("Pluie", Array(0.0), Array(0.0)).setMarker(SeriesMarkers.NONE)
    val swingWrapper = new SwingWrapper(chart)
    swingWrapper.displayChart()

    val xData = new ListBuffer[Double]()
    val yData1 = new ListBuffer[Double]()
    val yData2 = new ListBuffer[Double]()
    val yData3 = new ListBuffer[Double]()

    // Define date formatter
    val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    // Update the chart every 5 minutes
    val chartQuery = windowedDf.writeStream
      .foreachBatch { (batchDf: DataFrame, batchId: Long) =>
        // Log the content of the batch
        println(s"Batch $batchId:")
        batchDf.show()

        // Collect the rows and sort by window_start
        val sortedBatchDf = batchDf.orderBy("window_start")

        // Update the chart
        sortedBatchDf.select(explode($"rows").as("row")).select("row.*").orderBy("_c0").collect().foreach { row =>
          val dateTime = row.getAs[java.sql.Timestamp]("_c0").toLocalDateTime
          xData += dateTime.toEpochSecond(ZoneOffset.UTC).toDouble // Convert timestamp to epoch seconds
          yData1 += Option(row.getAs[Double]("signal_std")).getOrElse(0.0) // Handle null values
          yData2 += Option(row.getAs[Double]("signal_rad")).getOrElse(0.0) // Handle null values
          yData3 += Option(row.getAs[Int]("pluie")).map(_.toDouble).getOrElse(0.0) // Handle null values and convert to double

        }

        // Log the coordinates for debugging
        println("xData: " + xData.mkString(", "))
        println("yData1: " + yData1.mkString(", "))
        println("yData2: " + yData2.mkString(", "))
        println("yData3: " + yData3.mkString(", "))

        // Update the chart with new data
        chart.updateXYSeries("Signal STD", xData.toArray, yData1.toArray, null)
        chart.updateXYSeries("Signal RAD", xData.toArray, yData2.toArray, null)
        chart.updateXYSeries("Pluie", xData.toArray, yData3.toArray, null)
        swingWrapper.repaintChart()
      }
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    query1.awaitTermination()
    query2.awaitTermination()
    chartQuery.awaitTermination()
  }
}
