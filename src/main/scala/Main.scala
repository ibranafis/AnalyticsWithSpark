import java.util.Scanner
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    try {
      val spark: SparkSession = SparkSession.builder()
        .master("local[1]")
        .appName("Execute Queries on XML")
        .getOrCreate()

      val scanner = new Scanner(System.in)
      print("Enter the name of the file: ")
      val fileName = scanner.nextLine()
      var noOfFiles = 1
      if(fileName.equals("*"))
      {
        print("Enter the number of files: ")
        noOfFiles=scanner.nextInt()
      }

      scanner.nextLine()
      print("Enter the root tag: ")
      val rowTag=scanner.nextLine()
      XMLToParquet.convert(spark, fileName, rowTag)

      RenameParquet.rename(spark, noOfFiles)

      val noOfQueries = QueryOverParquet.query(spark)

      RenameOutput.rename(spark, noOfQueries)
    }
    catch {
      case ex: Exception => {
        println("Exception " + ex)
      }
    }
  }
}