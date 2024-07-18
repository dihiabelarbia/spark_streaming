import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object CheckCSVContent {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CheckCSVContent")
      .master("local[*]")
      .getOrCreate()

    val schema = new StructType()
      .add("_c0", StringType, true)
      .add("signal_std", DoubleType, true)
      .add("signal_rad", DoubleType, true)
      .add("pluie", IntegerType, true)

    // Lire les fichiers CSV
    val inputDf = spark.read
      .schema(schema)
      .option("header", "true")
      .csv("csvInter/cleanedDf")

    // Afficher le contenu
    println("Contenu de cleanedDf :")
    inputDf.show()

    val outputDf = spark.read
      .schema(schema)
      .option("header", "true")
      .csv("csvInter/interpolated_signals_with_outliers/")

    // Afficher le contenu
    println("Contenu de interpolated_signals_with_outliers :")
    outputDf.show()
  }
}
