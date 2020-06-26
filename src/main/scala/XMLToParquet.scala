import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml._

object XMLToParquet {
  def convert(spark: SparkSession, fileName: String, rowTag:String):Unit={

    val xmlDf= spark.read.option("rowTag", rowTag).xml("src/main/xmldata/"+fileName+".xml")

    xmlDf.write.mode("append").parquet("src/main/parquetdata")

  }
}