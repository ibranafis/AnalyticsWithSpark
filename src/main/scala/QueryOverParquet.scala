import java.util.Scanner
import org.apache.spark.sql.{SparkSession}

object QueryOverParquet {
  def query(spark: SparkSession): Int = {
    val parquetDF = spark.read.parquet("src\\main\\parquetdata\\file*.parquet")
    parquetDF.show()
    parquetDF.createOrReplaceTempView("parquetFile")

    val sc=new Scanner(System.in)
    print("Enter the number of queries: ")
    val noOfQueries=sc.nextInt()
    sc.nextLine()

    println("Enter the SQL Queries \n")
    for(i<-1 to noOfQueries)
      {
        val columns=parquetDF.columns.toSeq
        println("Columns are: " +columns)

        print("\nEnter the column names to be shown (Press * for all columns): ")
        val col=sc.nextLine()
        sc.nextLine()
        print("Enter the condition: ")
        val query=sc.nextLine()

        val sqlText="SELECT " + col + " FROM parquetFile WHERE " + query + ""
        val outputDF=spark.sql(sqlText)

        outputDF.coalesce(1).write.mode("append").option("header", "true").csv("src/main/output")
      }
    noOfQueries
  }
}