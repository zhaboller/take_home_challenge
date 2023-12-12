package take_home

import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.immutable.Seq
import com.typesafe.config.ConfigFactory

object TakeHome {
  def main(args: Array[String]): Unit = {
    // Load configuration settings
    val config = ConfigFactory.load("application.conf")
    val datasetPath = config.getString("takehome.paths.dataset")
    val logLevel = config.getString("takehome.logging.level")

    // Set log level for Spark and Akka to suppress verbose logs
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Initialize Spark Session
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("TakeHome")
      .getOrCreate()
    spark.sparkContext.setLogLevel(logLevel)

    // Load and display raw data
    val rawData = Data_Extraction.loadFromCSV(datasetPath)
    println(rawData.printSchema())
    println(rawData.show(15))
    println(rawData.describe().show())

    // Explore distinct values in each column
    rawData.columns.foreach { columnName =>
        val distinctCount = rawData.select(columnName).distinct().count()
        println(s"Column $columnName has $distinctCount distinct values")
    }
    
    // Clean the data
    val cleanedData = Data_Processing.cleanData(rawData)

    // Perform data aggregation
    Data_Aggregation.aggregateData(cleanedData)

    // Conduct time series analysis
    Time_Series_Analysis.analyze(cleanedData)

    // Stop the Spark session
    spark.stop()
  }
}
