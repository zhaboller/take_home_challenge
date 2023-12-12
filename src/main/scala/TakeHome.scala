package take_home
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import plotly._
import plotly.element._
import plotly.layout._
import plotly.Almond._
import scala.collection.immutable.Seq
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.apache.log4j.Level
object TakeHome {
  def main(args: Array[String]): Unit ={
    // Load the configuration
    val config = ConfigFactory.load("application.conf")
    val datasetPath = config.getString("takehome.paths.dataset")
    val logLevel = config.getString("takehome.logging.level")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("TakeHome")
      .getOrCreate()
    spark.sparkContext.setLogLevel(logLevel)
    val rawData = Data_Extraction.loadFromCSV(datasetPath)
    println(rawData.printSchema())
    println(rawData.show(15))
    println(rawData.describe().show())

    rawData.columns.foreach { columnName =>
        val distinctCount = rawData.select(columnName).distinct().count()
        println(s"Column $columnName has $distinctCount distinct values")
    }
    
    val cleanedData = Data_Processing.cleanData(rawData)

    Data_Aggregation.aggregateData(cleanedData)

    Time_Series_Analysis.analyze(cleanedData)
    

  // Stop the Spark session
    spark.stop()
  }
}