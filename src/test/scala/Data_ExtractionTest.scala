package take_home
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import take_home.Data_Extraction

class Data_ExtractionTest extends AnyFunSuite {
  val spark: SparkSession = SparkSession.builder()
    .appName("DataExtractionTest")
    .master("local[*]")
    .getOrCreate()
  test("Data_Extraction should load data correctly") {

    val testCsvPath = "src/test/resources/Dataset.csv"

    val df = Data_Extraction.loadFromCSV(testCsvPath)

    val expectedSchema = StructType(List(
      StructField("date", TimestampType, true),
      StructField("site_id", IntegerType, true),
      StructField("ad_type_id", IntegerType, true),
      StructField("geo_id", IntegerType, true),
      StructField("device_category_id", IntegerType, true),
      StructField("advertiser_id", IntegerType, true),
      StructField("order_id", IntegerType, true),
      StructField("line_item_type_id", IntegerType, true),
      StructField("os_id", IntegerType, true),
      StructField("integration_type_id", IntegerType, true),
      StructField("monetization_channel_id", IntegerType, true),
      StructField("ad_unit_id", IntegerType, true),
      StructField("total_impressions", IntegerType, true),
      StructField("total_revenue", DoubleType, true),
      StructField("viewable_impressions", IntegerType, true),
      StructField("measurable_impressions", IntegerType, true),
      StructField("revenue_share_percent", IntegerType, true)
    ))
    assert(df.schema.toList == expectedSchema)
  }
  spark.stop()
}