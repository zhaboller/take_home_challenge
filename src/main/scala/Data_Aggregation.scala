package take_home

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import plotly._
import plotly.element._
import plotly.layout._
import plotly.Almond._
import scala.collection.immutable.Seq

object Data_Aggregation {

    // Function to plot a scatter chart.
    def plotScatterChart(xValues: Seq[String], yValues: Seq[Double], title: String, outputFile: String): Unit = {
        val scatterPlot = Seq(
            Scatter(xValues, yValues, mode = ScatterMode(ScatterMode.Markers))
        )

        // Define the layout of the plot.
        val layout = Layout(title)

        // Render the plot to a file (HTML).
        plotly.Plotly.plot(outputFile, scatterPlot, layout)
    }

    // Function to plot a line chart.
    def plotLineChart(xValues: Seq[String], yValues: Seq[Double], title: String, outputFile: String): Unit = {
        val linePlot = Seq(
            Scatter(xValues, yValues, mode = ScatterMode(ScatterMode.Lines))
        )

        // Define the layout of the plot.
        val layout = Layout(title)

        // Render the plot to a file (HTML).
        plotly.Plotly.plot(outputFile, linePlot, layout)
    }

    // Function to aggregate data and produce visualizations.
    def aggregateData(df: DataFrame): Unit = {

        // Total Impressions by Site and Ad Type
        df.select("site_id", "ad_type_id", "total_impressions").show(5)
        val totalImpressionBySiteAdType = df.groupBy("site_id", "ad_type_id")
                                            .sum("total_impressions")
                                            .orderBy("site_id", "ad_type_id")
        totalImpressionBySiteAdType.show()

        val totalImpressionsData = totalImpressionBySiteAdType.collect()
        val siteIds = totalImpressionsData.map(_.getAs[Long]("site_id").toString)
        val adTypeIds = totalImpressionsData.map(_.getAs[Long]("ad_type_id").toString)
        val impressions = totalImpressionsData.map(_.getAs[Long]("sum(total_impressions)").toDouble).to[scala.collection.immutable.Seq]

        val siteAdType = siteIds.zip(adTypeIds).map { case (site, adType) => s"Site: $site, AdType: $adType" }.to[scala.collection.immutable.Seq]
        plotLineChart(siteAdType, impressions, "Impression across Site and Adtype", "reports/Impression_over_Site_and_Adtype.html")

        // Average Revenue per Advertiser
        df.select("advertiser_id", "total_revenue").show(5)
        val avgRevenueByAdvertiser = df.groupBy("advertiser_id")
                                       .avg("total_revenue")
                                       .withColumnRenamed("avg(total_revenue)", "avg_revenue")
                                       .orderBy("advertiser_id")
        avgRevenueByAdvertiser.show()

        val avgRevenueByAdvertiserFormatted = avgRevenueByAdvertiser.collect()
            .map(row => (row.getAs[Long]("advertiser_id").toString, row.getAs[Double]("avg_revenue")))

        val advertiserId = avgRevenueByAdvertiserFormatted.map(_._1).to[scala.collection.immutable.Seq]
        val totalRevenueAdv = avgRevenueByAdvertiserFormatted.map(_._2).to[scala.collection.immutable.Seq]

        plotScatterChart(advertiserId, totalRevenueAdv, "Total Revenue across Advertiser", "reports/Revenue_over_Advertiser.html")

        // Revenue Share by Monetization Channel
        df.select("monetization_channel_id", "total_revenue").show(5)
        val totalRevenuePerChannel = df.groupBy("monetization_channel_id")
                                       .agg(sum("total_revenue").alias("channel_revenue"))
        totalRevenuePerChannel.show()

        val totalRevenue = df.agg(sum("total_revenue").alias("total_revenue")).first().getAs[Double](0)

        val revenueShare = totalRevenuePerChannel.withColumn("percentage_share", 
                          round((col("channel_revenue") / totalRevenue) * 100, 4))
                          .orderBy("monetization_channel_id")
        revenueShare.show()


  }
}
