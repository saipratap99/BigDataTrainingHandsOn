import org.apache.spark.sql.SparkSession

object AggregateJob {
  def main(args: Array[String]): Unit ={
    val sparkSession = SparkSession.builder()
                                    .appName("AggregareJob")
                                    .master("local[*]")
                                    .getOrCreate();

    val sparkContext = sparkSession.sparkContext;
    val textFileRDD = sparkContext.textFile("hdfs://localhost:9000/auth/auth.csv")

    val asaRDD = textFileRDD.map(row => row.split(","))
                                .filter(row => !row(4).equalsIgnoreCase("asa"))
                                .map(row => (("asa",row(4).toInt))).repartition(3)

    val piUsesFlagRDD = textFileRDD.map(row => row.split(","))
                                    .filter(row => !row(4).equalsIgnoreCase("asa"))
                                    .map(row => (("pi_uses_flag",row(13).toInt))).repartition(1)

    val asaAndPiRDD = asaRDD.union(piUsesFlagRDD)
//    asaAndPiRDD.foreach(println)

    // ((asa_count, asa_sum, asa_avg)
    val aggregatedRDD = asaAndPiRDD.aggregateByKey((0, 0.0f, 0.0f))(
      (init, value) => ((init._1 + 1, init._2 + value , (init._2 + value) / (init._1 + 1))),
      (p1, p2) => ((p1._1 + p2._1, p1._2 + p2._2, (p1._2 + p2._2) / (p1._1 + p2._1)))
    )

    aggregatedRDD.foreach(each => println(s"${each._1} -> count: ${each._2._1}, sum: ${each._2._2}, avg: ${each._2._3}"))

    scala.io.StdIn.readLine();

  }
}

case class AuthDTO(asa: Int, pi_uses_flag: Int);
