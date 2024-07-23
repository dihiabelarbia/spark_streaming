import scala.io.Source
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

case class ARIMACoefficients(ar: Seq[Double], ma: Seq[Double], intercept: Double)

object ARIMAPredictor {
  implicit val formats: Formats = DefaultFormats

  def main(args: Array[String]): Unit = {
    // Lire les coefficients des fichiers JSON
    val radJsonStr = Source.fromFile("poidModel/model_rad_coefficients.json").getLines.mkString
    val stdJsonStr = Source.fromFile("poidModel/model_std_coefficients.json").getLines.mkString

    val radCoefficients = parse(radJsonStr).extract[ARIMACoefficients]
    val stdCoefficients = parse(stdJsonStr).extract[ARIMACoefficients]

    // Fonction de prédiction ARIMA
    def predictARIMA(data: Array[Double], coefficients: ARIMACoefficients): Double = {
      val arPart = coefficients.ar.zip(data.reverse).map { case (ar, d) => ar * d }.sum
      val maPart = coefficients.ma.zip(data.reverse).map { case (ma, d) => ma * d }.sum
      coefficients.intercept + arPart + maPart
    }

    // Simuler des données de test pour model_rad et model_std
    //val testDataRad = Array(10.0, 9.0, 8.0, 7.0, 6.0)
    //val testDataStd = Array(11.0, 10.0, 9.0, 8.0, 7.0)

    // Faire des prédictions
   //.val predictionStd = predictARIMA(testDataStd, stdCoefficients)

    //println(s"Prédiction pour model_rad : $predictionRad")
    //println(s"Prédiction pour model_std : $predictionStd")
  }
}
