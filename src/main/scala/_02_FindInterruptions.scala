import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object _02_FindInterruptions {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("FindInterruptions")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val filePath = "csvInter/cleanedDf"
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(filePath)

    val selectedColumnsDf = df.select("_c0", "signal_std", "signal_rad", "pluie")
    val dfWithTimestamp = selectedColumnsDf.withColumn("timestamp", unix_timestamp(col("_c0"), "yyyy-MM-dd HH:mm:ss").cast("timestamp"))

    // Ajouter une colonne pour indiquer la présence de NA
    val dfWithNaFlag = dfWithTimestamp.withColumn("is_na",
      col("signal_std").isNull || col("signal_rad").isNull || col("pluie").isNull)

    // Utiliser les fonctions lag et lead pour détecter les changements dans les périodes de NA
    val windowSpec = Window.orderBy("timestamp")
    val dfWithLagLead = dfWithNaFlag.withColumn("prev_is_na", lag("is_na", 1, false).over(windowSpec))
      .withColumn("next_is_na", lead("is_na", 1, false).over(windowSpec))

    // Détecter les débuts et les fins des périodes de NA
    val dfWithPeriods = dfWithLagLead.withColumn("start_na", when(col("is_na") && !col("prev_is_na"), col("timestamp")).otherwise(null))
      .withColumn("end_na", when(!col("is_na") && col("prev_is_na"), col("timestamp")).otherwise(null))

    // Propager les dates de début et de fin
    val dfWithFilledPeriods = dfWithPeriods.withColumn("start_na_filled", last("start_na", ignoreNulls = true).over(windowSpec))
      .withColumn("end_na_filled", last("end_na", ignoreNulls = true).over(windowSpec))

    // Filtrer les périodes de NA
    val periods = dfWithFilledPeriods.filter(col("start_na_filled").isNotNull && col("end_na_filled").isNotNull)
      .select("start_na_filled", "end_na_filled").distinct()

    // Calculer la durée des périodes d'interruption en minutes
    val periodsWithDuration = periods.withColumn("duration_minutes", (col("end_na_filled").cast("long") - col("start_na_filled").cast("long")) / 60)

    // Filtrer les périodes avec des durées valides
    val validPeriods = periodsWithDuration.filter(col("duration_minutes") > 0)

    // Classer les périodes par durée décroissante
    val sortedPeriods = validPeriods.orderBy(col("duration_minutes").desc)

    // Afficher les périodes d'interruption avec leur durée
    println("Périodes d'interruption avec leur durée:")
    sortedPeriods.show(false)

    // Calculer les statistiques des périodes d'interruption
    val maxDuration = sortedPeriods.agg(max("duration_minutes")).as[Double].first()
    val minDuration = sortedPeriods.agg(min("duration_minutes")).as[Double].first()
    val avgDuration = sortedPeriods.agg(avg("duration_minutes")).as[Double].first()

    println(s"Durée maximale d'une période d'interruption: ${maxDuration / 1440} jours (${maxDuration} minutes)")
    println(s"Durée minimale d'une période d'interruption: ${minDuration / 1440} jours (${minDuration} minutes)")
    println(s"Durée moyenne des périodes d'interruption: ${avgDuration / 1440} jours (${avgDuration} minutes)")

    // Fermer la session Spark
    spark.stop()
  }
}
