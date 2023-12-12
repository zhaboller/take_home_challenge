import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import plotly._, element._, layout._, Plotly._
val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("TakeHome")
      .getOrCreate()
var df = spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv("../Dataset.csv")

df.printSchema()
df.show(10)
df.describe().show()
df.filter(col("date").isNull).count()
def countMissingValues(columnName: String, df: DataFrame): Long = {
    df.filter(col(columnName).isNull || col(columnName).isNaN).count()
}
val columnsWithoutDate = df.columns.filter(_ != "date")
val missingValueCounts = columnsWithoutDate.map(colName => (colName, countMissingValues(colName, df)))
missingValueCounts.foreach { case (colName, count) =>
    println(s"Column $colName has $count missing values")
}

df.columns.foreach { columnName =>
  val distinctCount = df.select(columnName).distinct().count()
  println(s"Column $columnName has $distinctCount distinct values")
}

val counts = df.groupBy("ad_type_id").count()
counts.show()

import org.apache.spark.sql.functions._

val valueDistribution = df.groupBy("ad_type_id").count()
//val distributionData = valueDistribution.collect().map(row => (row.getString(0), row.getLong(1)))


val impressionsByDay = df.groupBy("date")
  .agg(sum("total_impressions").alias("daily_impressions"))
  .orderBy("date")
impressionsByDay.show()

//val columnDataArray = df.select("date").toSeq

//columnDataArray.show()

val timeSeriesData = impressionsByDay.collect().map(row => (row.getAs[java.sql.Date]("date"), row.getAs[Long]("daily_impressions"))).toSeq

// Separate the dates and impressions into two sequences.
val (dates, impressions) = timeSeriesData.unzip

// Convert dates to strings for plotting. Depending on the plotly-scala API version, you might need to use .toString.
val dateStrings = dates.map(_.toString)

// Create the time series plot.
val timeSeriesPlot = Seq(
  Scatter(
    dateStrings,
    impressions,
    mode = ScatterMode(ScatterMode.Markers)
  )
)

// Define the layout of the plot.
val layout = Layout().title("Daily Impressions Over Time")

// Render the plot to a file (HTML).
val plotFile = "impressions_time_series.html"
plotly.Plotly.plot(plotFile, timeSeriesPlot, layout)