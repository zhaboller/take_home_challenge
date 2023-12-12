package take_home
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConverters._
import take_home.Data_Extraction

class Data_ExtractionTest extends AnyFunSuite with BeforeAndAfterAll {
  var spark: SparkSession = _
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .appName("DataExtractionTest")
      .master("local[*]")
      .getOrCreate()
  }
  test("Data_Extraction should load data correctly") {
    val config = ConfigFactory.load()
    val testCsvPath = config.getString("takehome.paths.dataset")

    val df = Data_Extraction.loadFromCSV(testCsvPath)
    val schemaConfigList = config.getConfigList("takehome.schema.fields")
    val expectedSchema = StructType(schemaConfigList.asScala.map { fieldConfig =>
      val name = fieldConfig.getString("name")
      val dataType = fieldConfig.getString("type") match {
      case "timestamp" => TimestampType
      case "integer" => IntegerType
      case "double" => DoubleType
      // Add cases for other data types as needed
      case _ => StringType // Default case, can be adjusted as needed
      }
      val nullable = fieldConfig.getBoolean("nullable")
      StructField(name, dataType, nullable)
      }.toList)
    assert(df.schema.toList == expectedSchema)
  }
  override def afterAll(): Unit = {
    try {
      spark.stop()
    } finally {
      super.afterAll()
    }
  }
}
