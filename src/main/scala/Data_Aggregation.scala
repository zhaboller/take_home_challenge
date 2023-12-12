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
    def plotScatterChart(xValues: Seq[String], yValues: Seq[Double], title: String, outputFile: String): Unit = {
        val timeSeriesPlot = Seq(
            Scatter(
            xValues,
            yValues.map(_.toDouble),
            mode = ScatterMode(ScatterMode.Markers)
            )
        )

        // Define the layout of the plot.
        val layout = Layout(title)

        // Render the plot to a file (HTML).
        plotly.Plotly.plot(outputFile, timeSeriesPlot, layout)
    }
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
  def aggregateData(df: DataFrame): Unit = {
    // Logic to aggregate data
    df.select("site_id","ad_type_id","total_impressions").show(5)
    val total_impression_by_site_adtype = df.groupBy("site_id","ad_type_id").sum("total_impressions").orderBy("site_id","ad_type_id")
    total_impression_by_site_adtype.show()
    val totalImpressionsData = total_impression_by_site_adtype.collect()
    val siteIds = totalImpressionsData.map(_.getAs[Long]("site_id").toString).toSeq
    val adTypeIds = totalImpressionsData.map(_.getAs[Long]("ad_type_id").toString).toSeq
    val impressions = totalImpressionsData.map(_.getAs[Long]("sum(total_impressions)").toDouble).toSeq.to[scala.collection.immutable.Seq]
    val Site_Ad_tppe = siteIds.zip(adTypeIds).map { case (site, adType) => s"Site: $site, AdType: $adType" }.to[scala.collection.immutable.Seq]
    plotLineChart(Site_Ad_tppe, impressions, "Impression across Site and Adtype", "reports/Impression_over_Site_and_Adtype.html")

    df.select("advertiser_id","total_revenue").show(5)
    val avg_revenue_by_advertiser = df.groupBy("advertiser_id").avg("total_revenue").withColumnRenamed("avg(total_revenue)", "avg_revenue").orderBy("advertiser_id")
    avg_revenue_by_advertiser.show()
    // Reformat the daily impression
    val avg_revenue_by_advertiser_formated = avg_revenue_by_advertiser
        .withColumn("advertiser_id", col("advertiser_id"))
        .collect()
        .map(row => (row.getAs[Long]("advertiser_id").toString, row.getAs[Double]("avg_revenue")))
        .toSeq
    // Convert to immutable Seq explicitly and ensure dates are converted to strings
    val advertiser_id = avg_revenue_by_advertiser_formated.map(_._1).to[scala.collection.immutable.Seq]
    val immutable_Total_revenue = avg_revenue_by_advertiser_formated.map(_._2).to[scala.collection.immutable.Seq]

    plotScatterChart(advertiser_id, immutable_Total_revenue, "Total Revenue across Advertiser", "reports/Revenue_over_Advertiser.html")

  
    df.select("monetization_channel_id","total_revenue").show(5)
    val totalRevenuePerChannel = df.groupBy("monetization_channel_id")
                                  .agg(sum("total_revenue").alias("channel_revenue"))
    totalRevenuePerChannel.show()
    val totalRevenue = df.agg(sum("total_revenue").alias("total_revenue")).first().getAs[Double](0)

    val revenueShare = totalRevenuePerChannel.withColumn("percentage_share", 
                          round((col("channel_revenue") / totalRevenue) * 100, 4)).orderBy("monetization_channel_id")
    revenueShare.show()
    /*
    val monetization_channel_id = revenueShare.map(_._1).to[scala.collection.immutable.Seq]
    val immutable_shared_revenue_pct = revenueShare.map(_._3).to[scala.collection.immutable.Seq]
    plotLineChart(monetization_channel_id, immutable_shared_revenue_pct, "Shared revenue across Monetization channel", "Revenue_over_Monitization.html")
    */

  }
}