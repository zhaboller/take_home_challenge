package take_home
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object Data_Extraction {
    def loadFromCSV(filePath: String): DataFrame = {
        val spark = SparkSession
        .builder
        .master("local[*]")
        .appName("TakeHome")
        .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        
        var df = spark.read.option("header", "true")
                    .option("inferSchema", "true")
                    .csv(filePath).cache()
        df
  }
  
}
