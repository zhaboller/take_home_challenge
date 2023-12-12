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
    def DropCol(df: DataFrame): DataFrame = {
        // Since revenue_share_percent and integration_type_id only have one distinct value, we can remove those two features
        var df_clean = df.drop("revenue_share_percent", "integration_type_id")
        df_clean
    }
    def ConvertFormat(df: DataFrame): DataFrame = {
        // Since revenue_share_percent and integration_type_id only have one distinct value, we can remove those two features
        var df_clean = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd HH:mm:ss"))
        df_clean
    }
    def dropDup(df: DataFrame): DataFrame = {
        // Check and Remove Duplicates
        var df_clean = df.dropDuplicates()
        df_clean
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
        var df_clean = DropCol(df)
        // Convert Date Format
        df_clean = ConvertFormat((df_clean))
        // Check and Remove Duplicates
        df_clean = dropDup(df_clean)
        df_clean.describe().show()
        df_clean
    }
}