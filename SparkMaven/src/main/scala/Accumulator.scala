import org.apache.spark.sql.SparkSession

object Accumulator {
  def main(args: Array[String]): Unit = {
      val sparkSession = SparkSession.builder()
        .appName("AccumulatorJob")
        .master("local[*]")
        .getOrCreate();

      val sparkContext = sparkSession.sparkContext;
      val textFileRDD = sparkContext.textFile("hdfs://localhost:9000/auth/auth.csv")

      val asaRDD = textFileRDD.map(row => row.split(","))
        .filter(row => !row(4).equalsIgnoreCase("asa"))
        .map(row => (("asa", row(4).toInt))).repartition(3)

      val asaAccumulator = sparkContext.longAccumulator("AsaSum");

      asaRDD.foreach(each => asaAccumulator.add(each._2))

      println(asaAccumulator.value)

  }
}
