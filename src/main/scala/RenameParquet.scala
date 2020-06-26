import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object RenameParquet {
  def rename(spark: SparkSession, noOfFiles: Int): Unit = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val basePath = "src/main/parquetdata/"

    for(i<- 1 to noOfFiles) {
      val file = fs.globStatus(new Path(basePath + "part*"))(0).getPath.getName
      fs.rename(new Path(basePath + file), new Path(basePath + s"file$i.parquet"))
    }
  }
}