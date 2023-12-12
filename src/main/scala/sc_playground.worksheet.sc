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

val totalRevenuePerChannel = df.groupBy("monetization_channel_id")
                                 .agg(sum("total_revenue").alias("channel_revenue"))
totalRevenuePerChannel.show()
val totalRevenue = df.agg(sum("total_revenue").alias("total_revenue")).first().getAs[Double](0)

val revenueShare = totalRevenuePerChannel.withColumn("percentage_share", 
                      round((col("channel_revenue") / totalRevenue) * 100, 4)).orderBy("monetization_channel_id")

revenueShare.show()
revenueShare.map(_._2).show()

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


// Convert the date column to Date type
val revenueByDayWithDate = revenueByDay.withColumn("date", to_date(col("date")))

// Extract day of the week from the date
val revenueByDayWithDayOfWeek = revenueByDayWithDate.withColumn("day_of_week", date_format(col("date"), "EEEE"))

// Collect the data and create a new DataFrame
val newDataset = revenueByDayWithDayOfWeek.select("date", "day_of_week", "daily_revenue")

// Show the new dataset
println(newDataset)


// Convert the date column to Date type
val impressionsByDayWithDate = impressionsByDay.withColumn("date", to_date(col("date")))
// Extract day of the week from the date
val impressionsByDayWithDayOfWeek = impressionsByDayWithDate.withColumn("day_of_week", date_format(col("date"), "EEEE"))
// Collect the data and create a new DataFrame called detailed_daily_impression
val detailed_daily_impression = impressionsByDayWithDayOfWeek.select("date", "day_of_week", "daily_impressions")
detailed_daily_impression.show(30)
detailed_daily_impression.select("daily_impressions").describe().show()

df.select("monetization_channel_id","total_revenue").show(5)

totalRevenuePerChannel.show()

