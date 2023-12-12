package take_home

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import plotly._
import plotly.element._
import plotly.layout._
import plotly.Almond._
import scala.collection.immutable.Seq

object Time_Series_Analysis {

    // Function to plot a line chart.
    def plotLineChart(xValues: Seq[String], yValues: Seq[Double], title: String, outputFile: String): Unit = {
        val timeSeriesPlot = Seq(
            Scatter(xValues, yValues, mode = ScatterMode(ScatterMode.Lines))
        )

        val layout = Layout(title)
        plotly.Plotly.plot(outputFile, timeSeriesPlot, layout)
    }

    def movingAvg(impressionsByDay: DataFrame): DataFrame = {
        val windowSpec = Window.orderBy("date").rowsBetween(-7, 0)
        val movingAvg = impressionsByDay.withColumn("moving_avg", avg("daily_impressions").over(windowSpec))
        movingAvg.show(30)
        val movingAvgPlt = movingAvg.collect()
                                    .map(row => (row.getAs[java.sql.Date]("date").toString, 
                                                row.getAs[Double]("moving_avg")))

        plotLineChart(movingAvgPlt.map(_._1).to[scala.collection.immutable.Seq], 
                movingAvgPlt.map(_._2).to[scala.collection.immutable.Seq],
            "7-day Moving Average of Daily Impressions Over Time", 
            "reports/7_Day_MovAvg_Impression.html")
        movingAvg
    }

    def dailyImpression(impressionsByDay: DataFrame): Unit = {
        val timeSeriesData = impressionsByDay
            .withColumn("date", to_date(col("date")))
            .collect()
            .map(row => (row.getAs[java.sql.Date]("date").toString, row.getAs[Long]("daily_impressions").toDouble))

        plotLineChart(timeSeriesData.map(_._1).to[scala.collection.immutable.Seq], 
                      timeSeriesData.map(_._2).to[scala.collection.immutable.Seq],
                    "Daily Impressions Over Time", 
                    "reports/impressions_time_series.html")

        val detailedDailyImpression = impressionsByDay
            .withColumn("day_of_week", date_format(col("date"), "EEEE"))
            .select("date", "day_of_week", "daily_impressions")
        detailedDailyImpression.show(30)
    }

    def dailyRevenue(df: DataFrame): Unit = {
        // Total revenue per day
        val revenueByDay = df.groupBy("date")
            .agg(sum("total_revenue").alias("daily_revenue"))
            .orderBy("date")

        // Collect and format daily revenue data
        val timeSeriesDataRev = revenueByDay
            .withColumn("date", to_date(col("date")))
            .collect()
            .map(row => (row.getAs[java.sql.Date]("date").toString, row.getAs[Double]("daily_revenue")))

        plotLineChart(timeSeriesDataRev.map(_._1).to[scala.collection.immutable.Seq], 
                      timeSeriesDataRev.map(_._2).to[scala.collection.immutable.Seq], 
                      "Daily Revenue Over Time", 
                      "reports/revenue_time_series.html")
    }
    def analyze(df: DataFrame): Unit = {
        // Total Impression per day
        val impressionsByDay = df.groupBy("date")
            .agg(sum("total_impressions").alias("daily_impressions"))
            .orderBy("date")

        val movingAvgDF = movingAvg(impressionsByDay)
        dailyImpression(impressionsByDay)
        dailyRevenue(df)

    }
}
