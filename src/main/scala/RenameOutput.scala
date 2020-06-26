import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object RenameOutput {
  def rename(spark: SparkSession, noOfQueries: Int): Unit = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val basePath = "src/main/output/"

    for (i <- 1 to noOfQueries) {
      val fileName = fs.globStatus(new Path(basePath + "part*"))(0).getPath.getName
      fs.rename(new Path(basePath + fileName), new Path(basePath + i + ".csv"))
    }
  }
}