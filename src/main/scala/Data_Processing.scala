package take_home
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import plotly._
import plotly.element._
import plotly.layout._
import plotly.Almond._
import scala.collection.immutable.Seq

object Data_Processing {
    def plotLineChart(xValues: Seq[String], yValues: Seq[Double], title: String, outputFile: String): Unit = {
          val timeSeriesPlot = Seq(
              Scatter(
              xValues,
              yValues.map(_.toDouble),
              mode = ScatterMode(ScatterMode.Lines)
              )
          )

          // Define the layout of the plot.
          val layout = Layout(title)

          // Render the plot to a file (HTML).
          plotly.Plotly.plot(outputFile, timeSeriesPlot, layout)
      }
  def cleanData(df: DataFrame): DataFrame = {
    // Logic to clean data
    val spark = SparkSession
        .builder
        .master("local[*]")
        .appName("TakeHome")
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    // Since revenue_share_percent and integration_type_id only have one distinct value, we can remove those two features
    var df_clean = df.drop("revenue_share_percent", "integration_type_id")
    // Convert Date Format
    df_clean = df_clean.withColumn("date", to_date(col("date"), "yyyy-MM-dd HH:mm:ss"))
    // Check and Remove Duplicates
    df_clean = df_clean.dropDuplicates()
    df_clean.describe().show()
    df_clean
  }
}