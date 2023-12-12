package take_home
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConverters._
import take_home.Data_Processing
class Data_ProcessingTest extends AnyFunSuite {
  val spark: SparkSession = SparkSession.builder()
    .appName("DataProcessingTest")
    .master("local[*]")
    .getOrCreate()
  import spark.implicits._

  test("Data_Processing should drop unused columns correctly") {
    val testData = Seq(
      ("2023-03-01 00:00:00", 1.0, "A"),
      ("2023-03-02 00:00:00", 1.0, "B")
    ).toDF("date", "revenue_share_percent", "integration_type_id")

    val result = Data_Processing.DropCol(testData)
    assert(!result.columns.contains("revenue_share_percent"))
    assert(!result.columns.contains("integration_type_id"))
  }

  test("Data_Processing should convert Date Format correctly") {
    val testData = Seq(
      ("2023-03-01 00:00:00", "A"),
      ("2023-03-02 00:00:00", "B")
    ).toDF("date", "data")

    val result = Data_Processing.ConvertFormat(testData)
    val expectedDateType = DataTypes.DateType
    assert(result.schema("date").dataType == expectedDateType)
  }

  test("Data_Processing should remove duplicates data correctly") {
    val testData = Seq(
      ("2023-03-01 00:00:00", "A"),
      ("2023-03-01 00:00:00", "A"),
      ("2023-03-02 00:00:00", "B")
    ).toDF("date", "data")

    val result = Data_Processing.dropDup(testData)
    assert(result.count() == testData.dropDuplicates().count())
  }
}