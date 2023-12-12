package take_home
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import plotly._
import plotly.element._
import plotly.layout._
import plotly.Almond._
import scala.collection.immutable.Seq


object Time_Series_Analysis {
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
    def analyze(df: DataFrame): Any = {
        // Total Impression per day
        val impressionsByDay = df.groupBy("date")
        .agg(sum("total_impressions").alias("daily_impressions"))
        .orderBy("date")

        val windowSpec = org.apache.spark.sql.expressions.Window.orderBy("date").rowsBetween(-7, 0) // 7-day moving average
        val movingAvg = impressionsByDay.withColumn("moving_avg", avg("daily_impressions").over(windowSpec))
        movingAvg.show()

        // Reformat the daily impression
        val timeSeriesData = impressionsByDay
            .withColumn("date", to_date(col("date")))
            .collect()
            .map(row => (row.getAs[java.sql.Date]("date").toString, row.getAs[Long]("daily_impressions").toDouble))
            .toSeq
        // Convert to immutable Seq explicitly and ensure dates are converted to strings
        val dateStrings = timeSeriesData.map(_._1).to[scala.collection.immutable.Seq]
        val immutableImpressions = timeSeriesData.map(_._2).to[scala.collection.immutable.Seq]

        plotLineChart(dateStrings, immutableImpressions, "Daily Impressions Over Time", "impressions_time_series.html")


        // Total revenue per day
        val revenueByDay = df.groupBy("date")
        .agg(sum("total_revenue").alias("daily_revenue"))
        .orderBy("date")
        val timeSeriesData_rev = revenueByDay
            .withColumn("date", to_date(col("date")))
            .collect()
            .map(row => (row.getAs[java.sql.Date]("date").toString, row.getAs[Double]("daily_revenue")))
            .toSeq
        // Convert to immutable Seq explicitly and ensure dates are converted to strings
        val dateStrings_rev = timeSeriesData_rev.map(_._1).to[scala.collection.immutable.Seq]
        val immutableRevenue = timeSeriesData_rev.map(_._2).to[scala.collection.immutable.Seq]

        plotLineChart(dateStrings, immutableRevenue, "Daily Revenue Over Time", "revenue_time_series.html")
    }

}
