import org.apache.spark.sql.SparkSession

object SortJob {
  def main(args: Array[String]): Unit ={
    val sparkSession = SparkSession.builder()
      .appName("SortJob")
      .master("local[*]")
      .getOrCreate();

    val sparkContext = sparkSession.sparkContext;
    val textFileRDD = sparkContext.textFile("hdfs://localhost:9000/auth/auth.csv")

    val authDTORDD = textFileRDD.map(row => row.split(","))
      .filter(row => !row(4).equalsIgnoreCase("asa"))
      .map(row => ((row(4).toInt, row(13).toInt))).repartition(1)


    authDTORDD.sortByKey().foreach(println);


  }
}
