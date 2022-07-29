import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType

object DataFrameOp {
  def main(args: Array[String]): Unit ={
    val sparkSession = SparkSession.builder()
      .master("local[*]").appName("DataFrameOp").getOrCreate()

    val csvDataFrame = sparkSession.
      read.
      option("header", true)
      .csv("/home/hadoop/Downloads/auth.csv")
      .withColumn("aua", col("aua").cast(IntegerType))
      .withColumn("asa", col("asa").cast(IntegerType))

    println(csvDataFrame.stat.cov("aua","asa"))

  }

}
