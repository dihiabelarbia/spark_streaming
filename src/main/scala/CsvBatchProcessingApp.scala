import org.apache.spark.sql.{SaveMode, SparkSession}

object CsvBatchProcessingApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("CSV Batch Processing Application")
      .master("local[*]")
      .getOrCreate()

    val inputFilePath = "C:\\Users\\dyhia\\Downloads\\CIV-00001.csv"
    val outputFilePath = "output/csv"

    val csvDF = spark.read.option("header", "true").csv(inputFilePath)

    val lines = csvDF.collect().toSeq
    val batchSize = 100
    val numBatches = (lines.length + batchSize - 1) / batchSize

    val schema = csvDF.schema

    for (i <- 0 until numBatches) {
      val start = i * batchSize
      val end = math.min((i + 1) * batchSize, lines.length)
      val batch = lines.slice(start, end)

      val batchDF = spark.createDataFrame(spark.sparkContext.parallelize(batch), schema)

      val batchOutputPath = s"$outputFilePath/batch_$i.csv"
      batchDF.write
        .option("header", "true")
        .mode(SaveMode.Overwrite)
        .csv(batchOutputPath)

      new java.io.PrintWriter("output/signal.txt") { write(s"batch_$i\n"); close() }

      Thread.sleep(10000)
    }

    spark.stop()
  }
}
